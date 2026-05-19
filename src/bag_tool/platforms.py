"""Platform registry for ground-truth alignment.

Each platform describes how its GPS-equivalent ground-truth pose is published and how
it relates to the VIO frame. The align command uses this to branch its GPS reader and
to skip platform-specific workflows (ArUco, raw RTK topic emission, ref_bag).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Literal

from rosbags.rosbag2 import Reader


@dataclass(frozen=True)
class PlatformConfig:
    name: str
    gps_topic: str
    # Supported GT message types. Dispatch happens at read time based on the actual
    # connection msgtype found in the bag — a platform may accept several variants
    # of the same sensor's output (e.g., altair supports both PoseStamped and
    # PointStamped flavors of /pf_geo_loc/fc_local_position).
    gps_msg_types: tuple[str, ...]
    yaw_source: Literal['rtk_topic', 'pose_quaternion']
    yaw_topic: str | None
    coord_frame: Literal['geodetic', 'ned', 'enu']
    position_frame_offset_rad: float
    detect_topics: tuple[str, ...]
    aruco_supported: bool
    emit_raw_rtk_topics: bool
    # Residual yaw bias added to the first-fix alignment (around world-Z). Measured
    # empirically when the first-fix method leaves a constant rotational offset between
    # GT and VIO trajectories. See per-platform values below for measurement context.
    yaw_correction_rad: float = 0.0
    # Per-msgtype overrides. When the bag's GT msgtype is a key here, that value
    # REPLACES yaw_correction_rad for the run. Needed when different flavors of the
    # same sensor's output need different corrections (e.g., altair's PointStamped,
    # which lacks per-sample orientation and so picks up a different first-fix yaw
    # than the PoseStamped variant).
    yaw_correction_rad_by_msgtype: dict[str, float] = field(default_factory=dict)


PLATFORMS: dict[str, PlatformConfig] = {
    'prince': PlatformConfig(
        name='prince',
        gps_topic='/m300/rtk/fix',
        gps_msg_types=('sensor_msgs/msg/NavSatFix',),
        yaw_source='rtk_topic',
        yaw_topic='/m300/rtk/yaw',
        coord_frame='geodetic',
        position_frame_offset_rad=-math.pi / 2,
        detect_topics=('/m300/rtk/fix',),
        aruco_supported=True,
        emit_raw_rtk_topics=True,
    ),
    'altair': PlatformConfig(
        name='altair',
        gps_topic='/pf_geo_loc/fc_local_position',
        # The same sensor publishes either a full pose (with quaternion) or a
        # position-only point, depending on the recorder configuration. Both are
        # accepted; the reader dispatches by the actual msgtype.
        gps_msg_types=('geometry_msgs/msg/PoseStamped',
                       'geometry_msgs/msg/PointStamped'),
        yaw_source='pose_quaternion',
        yaw_topic=None,
        coord_frame='ned',
        position_frame_offset_rad=0.0,
        detect_topics=('/pf_geo_loc/fc_local_position',),
        aruco_supported=False,
        emit_raw_rtk_topics=False,
        # TODO: root-cause these residual yaw biases and remove the constants.
        # PoseStamped (-0.213286): measured on Day20.7 over [80,110]s — RMSE 12.1→3.7 m.
        # PointStamped (-2.337944): measured on Day20.1+Day20.2 over a 20s segment
        #   starting at ascension-end (drone has finished climbing, starts moving
        #   horizontally) — Day20.1 RMSE 55.2→4.2 m, Day20.2 RMSE 63.0→9.0 m. The
        #   large delta from PoseStamped reflects that PointStamped lacks per-sample
        #   orientation, so the first-fix uses a constant NED→ENU surrogate and ends
        #   up rotated very differently from the PoseStamped path. Mean of the two
        #   bags is -2.124658 rad relative to current -0.213286, so total = -2.337944.
        yaw_correction_rad=-0.213286,
        yaw_correction_rad_by_msgtype={
            'geometry_msgs/msg/PoseStamped':  -0.213286,
            'geometry_msgs/msg/PointStamped': -2.337944,
        },
    ),
}


def auto_detect(reader_topics: Iterable[str]) -> PlatformConfig:
    topics = set(reader_topics)
    matches = [
        cfg for cfg in PLATFORMS.values()
        if all(t in topics for t in cfg.detect_topics)
    ]
    if not matches:
        raise ValueError(
            f"cannot auto-detect platform; specify --platform "
            f"(known platforms: {', '.join(PLATFORMS)})"
        )
    if len(matches) > 1:
        names = ', '.join(m.name for m in matches)
        raise ValueError(
            f"platform auto-detection is ambiguous ({names}); specify --platform"
        )
    return matches[0]


def detect_from_bag(input_bag: str | Path) -> PlatformConfig:
    """Open the bag, read its connection list, and dispatch to auto_detect."""
    return detect_from_bags([input_bag])


def detect_from_bags(bags: Iterable[str | Path]) -> PlatformConfig:
    """Auto-detect the platform from the union of topics across multiple bags.

    Use this when (e.g.) the align subcommand has both an input bag (which may be
    an OpenVINS output stripped of the source GPS topic) and a ref_bag (the original
    recording). The GT topic that identifies the platform may only live in one of them.
    """
    topics: list[str] = []
    for bag in bags:
        if bag is None:
            continue
        path = Path(bag)
        reader_path = path.parent if path.is_file() else path
        with Reader(reader_path) as reader:
            topics.extend(c.topic for c in reader.connections)
    return auto_detect(topics)
