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
    yaw_source: Literal['rtk_topic', 'pose_quaternion', 'mag_ground']
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
    # Alignment method used when --align-method is not given. 'dynamic' (bag_tool.dynamic_align) recovers the yaw between the VIO and the ground truth
    # from the two paths, for platforms whose ground truth carries no usable heading; 'first-fix' needs the heading (RTK/ArUco yaw, or GT orientation).
    default_align_method: str = 'first-fix'
    # mag_ground platforms (no heading in the GT): the yaw comes from the magnetometer on the
    # ground. m_imu = mag_sign * R(mag_to_imu_rotvec) @ (m_uT - mag_hard_iron_ut), true north =
    # magnetic north + mag_declination_deg (east positive). ground_window_s: the static window at
    # the start of the VIO output (raven writes its pre-start static pose there, motors off).
    mag_to_imu_rotvec: tuple[float, float, float] = (0.0, 0.0, 0.0)
    mag_hard_iron_ut: tuple[float, float, float] = (0.0, 0.0, 0.0)
    mag_sign: float = 1.0
    mag_declination_deg: float = 0.0
    ground_window_s: float = 5.0
    # 'gnss-ins' alignment: the ground truth is a GNSS/INS smoother over the ORIGINAL recording (align --gt-bag),
    # gt_imu_topic (sensor_msgs/Imu) + gt_pvt_topic (lat/lon/height, NED Doppler velocity, h/v accuracy); the full
    # 6-DoF GT trajectory is emitted at gnss_ins_out_hz.
    gt_imu_topic: str | None = None
    gt_pvt_topic: str | None = None
    gnss_ins_out_hz: float = 50.0
    # gnss-ins height source: 'gnss' = receiver altitude in the position update; 'doppler' = horizontal position only,
    # height from the Doppler vertical velocity (align --gt-height overrides)
    gnss_ins_height: str = 'gnss'


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
        default_align_method='dynamic',      # position-only FC/GPS ground truth, no heading: the first-fix constants only fit the Day20 take-off heading
        yaw_correction_rad_by_msgtype={
            'geometry_msgs/msg/PoseStamped':  -0.213286,
            'geometry_msgs/msg/PointStamped': -2.337944,
        },
    ),
    # PAS-simulated missions from eagle-nest (pas_native_* bags). Publishes the
    # same GT topic as altair but the GT is exact sim kinematics in a clean NED
    # frame — no casket-mount yaw bias, so NO yaw_correction (applying altair's
    # -12.22 deg hack to sim bags inflated reported ATE ~3x). Distinguished from
    # altair by the sim-only /camera/depth topic; that topic lives in the INPUT
    # (ref) bag, not the VIO result bag, so auto-detection needs the ref bag
    # passed too (rosassist does) — otherwise pass --platform alexios.
    'alexios': PlatformConfig(
        name='alexios',
        gps_topic='/pf_geo_loc/fc_local_position',
        gps_msg_types=('geometry_msgs/msg/PoseStamped',),
        yaw_source='pose_quaternion',
        yaw_topic=None,
        coord_frame='ned',
        position_frame_offset_rad=0.0,
        detect_topics=('/pf_geo_loc/fc_local_position', '/camera/depth'),
        aruco_supported=False,
        emit_raw_rtk_topics=False,
        yaw_correction_rad=0.0,
    ),
    # ezio rig (Jetson + lee_capture_node, little_eagle_msgs): u-blox NEO-M8N single antenna on
    # /gnss/fix, no heading -> 'ground-mag' alignment: position = mean GNSS fix over the static
    # ground window, yaw = IST8310 magnetometer (/mag/field) over the same window, tilt from the
    # VIO's own gravity-aligned attitude. No trajectory fit.
    # Magnetometer calibration (2026-11-14, ezio_mag_check.py on lee_2026-11-14_16-26-48 against
    # raven's attitude): median residual 0.85 uT in flight / ~1 uT on the ground (~2.6 deg per uT
    # of horizontal field); the IST8310 field reads sign-reversed in a right-handed fit (mag_sign
    # -1). Ground yaw from it vs the GNSS-track yaw: within 1.4 deg on 16-26-48 (motors off) and
    # 0-4 deg on 15-47-28 (calibration from the other flight). Armed motors on the ground disturb it
    # by up to ~7 deg -> only the pre-arm window is used. Declination +5.9 deg (Timisoara, WMM 2026).
    # RISK: hard iron changes when the rig's wiring/battery placement changes -> re-fit then.
    'ezio': PlatformConfig(
        name='ezio',
        gps_topic='/gnss/fix',
        gps_msg_types=('sensor_msgs/msg/NavSatFix',),
        yaw_source='mag_ground',
        yaw_topic='/mag/field',
        coord_frame='geodetic',
        position_frame_offset_rad=0.0,
        detect_topics=('/gnss/fix', '/mag/field'),
        aruco_supported=False,
        emit_raw_rtk_topics=False,
        default_align_method='gnss-ins',
        mag_to_imu_rotvec=(6.58352, 6.69978, -0.40537),
        mag_hard_iron_ut=(-6.60, -2.73, -35.47),
        mag_sign=-1.0,
        mag_declination_deg=5.9,
        ground_window_s=5.0,
        # default alignment 'gnss-ins' (2026-11-16): GNSS/INS smoother on the ICM-42688 (best gyro) + u-blox PVT from the
        # recording -> full-pose GT (RTE scorable) and a GNSS/INS heading instead of the magnetometer. 'ground-mag' (the
        # previous default) stays available; it gives a position-only GT (RTE n/a, RTE-D only).
        gt_imu_topic='/imu2/data_raw',
        gt_pvt_topic='/gnss/pvt',
        # GT height from the Doppler vertical velocity (2026-11-19): the M8N receiver altitude slips (ended 5.5 m below
        # take-off on 16-26-48 while baro, Doppler height and the VIO all agreed on ~0) -> fake VIO vertical drift.
        gnss_ins_height='doppler',
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
        # Prefer the most specific match: a platform whose detect_topics are a
        # strict superset of every other match's (e.g. alexios sim bags also
        # carry altair's GT topic). Only raise when specificity ties.
        matches.sort(key=lambda cfg: len(cfg.detect_topics), reverse=True)
        top = set(matches[0].detect_topics)
        if all(set(m.detect_topics) < top for m in matches[1:]):
            return matches[0]
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
