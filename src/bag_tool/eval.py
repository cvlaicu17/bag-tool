"""eval: compute RTE metrics from an already-aligned MCAP bag."""

from __future__ import annotations

import bisect
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader
from rosbags.typesys import get_typestore

from bag_tool.add_topics import _reader_path
from bag_tool.processor import JUMP_THRESHOLD, rms_jump_penalty, rte_values_from_ate

_RTK_TOPIC = '/ov_srvins/rtk/pose_aligned'
_VIO_TOPIC = '/ov_srvins/vio/pose'
_POSE_TYPE = 'geometry_msgs/msg/PoseWithCovarianceStamped'


def run(input_bag: str, stores_enum, rte_window_ns: int = 1_000_000_000) -> None:
    """Read aligned MCAP bag and compute RTE metrics."""
    typestore = get_typestore(stores_enum)
    input_path = Path(input_bag)
    reader_path = _reader_path(input_path)

    rtk_poses: list[tuple[int, int, np.ndarray]] = []  # (bag_ts, header_ns, pos)
    vio_poses: list[tuple[int, int, np.ndarray]] = []

    with Reader(reader_path) as reader:
        relevant = [c for c in reader.connections if c.topic in {_RTK_TOPIC, _VIO_TOPIC}]
        if not relevant:
            print(f'ERROR: no aligned pose topics found in {reader_path}')
            return
        for conn, ts, rawdata in reader.messages(connections=relevant):
            msg = typestore.deserialize_cdr(rawdata, conn.msgtype)
            stamp_ns = msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec
            pos = np.array([
                msg.pose.pose.position.x,
                msg.pose.pose.position.y,
                msg.pose.pose.position.z,
            ])
            if conn.topic == _RTK_TOPIC:
                rtk_poses.append((ts, stamp_ns, pos))
            else:
                vio_poses.append((ts, stamp_ns, pos))

    if not rtk_poses or not vio_poses:
        print(f'ERROR: aligned bag is missing {_RTK_TOPIC!r} or {_VIO_TOPIC!r}')
        return

    print(f'Loaded {len(rtk_poses)} RTK poses, {len(vio_poses)} VIO poses')

    al_stamps    = [s for _, s, _ in rtk_poses]
    al_positions = [p for _, _, p in rtk_poses]
    n_al = len(al_stamps)

    def _nearest(vio_stamp_ns: int) -> int:
        idx = bisect.bisect_left(al_stamps, vio_stamp_ns)
        if idx == 0:
            return 0
        if idx >= n_al:
            return n_al - 1
        return idx if (al_stamps[idx] - vio_stamp_ns) <= (vio_stamp_ns - al_stamps[idx - 1]) else idx - 1

    ate_records: list[tuple[int, int, float]] = [
        (vio_ts, vio_stamp_ns, float(np.linalg.norm(vio_pos - al_positions[_nearest(vio_stamp_ns)])))
        for vio_ts, vio_stamp_ns, vio_pos in vio_poses
    ]

    rte_values = rte_values_from_ate(ate_records, rte_window_ns)
    rms_rte, jump_penalty = rms_jump_penalty(rte_values)
    print(f'RMS RTE      : {rms_rte:.4f} m')
    print(f'Jump penalty : {jump_penalty:.4f} m  (threshold={JUMP_THRESHOLD}m)')
