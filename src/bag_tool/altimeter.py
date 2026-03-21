"""Compare altimeter range vs RTK altitude."""

from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore, Stores

from bag_tool.trim import _rihs01

_RTK_FIX_TOPIC    = '/m300/rtk/fix'
_ALTIMETER_TOPIC  = '/altimeter/range'
_RTK_ALT_TOPIC    = '/m300/rtk/altitude_relative'
_DIFF_TOPIC       = '/altimeter_vs_rtk'


def run(input_bag: str, output_bag: str) -> None:
    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    # Bag was recorded with Humble (no variance field in Range); use Jazzy only for output types
    ts_read  = get_typestore(Stores.ROS2_HUMBLE)
    ts_write = get_typestore(Stores.ROS2_JAZZY)

    # ---- pass 1: collect RTK altitudes and altimeter ranges ----
    rtk_times: list[int] = []
    rtk_alts: list[float] = []
    alt_times: list[int] = []
    alt_ranges: list[float] = []
    alt_raw: list[bytes] = []
    alt_conn_info: dict = {}

    with Reader(reader_path) as reader:
        for conn, ts, raw in reader.messages():
            if conn.topic == _RTK_FIX_TOPIC:
                try:
                    msg = ts_read.deserialize_cdr(raw, conn.msgtype)
                except Exception:
                    continue  # skip malformed first message
                rtk_times.append(ts)
                rtk_alts.append(msg.altitude)
            elif conn.topic == _ALTIMETER_TOPIC:
                try:
                    msg = ts_read.deserialize_cdr(raw, conn.msgtype)
                except Exception:
                    continue  # skip malformed messages
                alt_times.append(ts)
                alt_ranges.append(msg.range)
                alt_raw.append(raw)
                if not alt_conn_info:
                    msgdef_str = conn.msgdef[1] if conn.msgdef else ''
                    alt_conn_info = dict(
                        msgtype=conn.msgtype,
                        msgdef=msgdef_str,
                        rihs01=conn.digest or _rihs01(msgdef_str),
                        serialization_format=conn.ext.serialization_format,
                        offered_qos_profiles=conn.ext.offered_qos_profiles,
                    )

    if len(rtk_times) < 2:
        print('ERROR: need at least 2 RTK fix messages', file=sys.stderr)
        sys.exit(1)

    # discard first RTK message, normalise relative to second
    rtk_times = rtk_times[1:]
    rtk_alts  = rtk_alts[1:]
    origin_alt = rtk_alts[0]
    rtk_rel = [a - origin_alt for a in rtk_alts]

    print(f'RTK altitude origin  : {origin_alt:.3f} m')
    print(f'RTK messages kept    : {len(rtk_times)}')
    print(f'Altimeter messages   : {len(alt_times)}')

    # interpolate normalised RTK altitude at each altimeter timestamp
    rtk_interp = np.interp(alt_times, rtk_times, rtk_rel)
    diffs = [a - r for a, r in zip(alt_ranges, rtk_interp)]

    # ---- pass 2: write output bag ----
    Float64 = ts_write.types['std_msgs/msg/Float64']
    msgdef_f64, _ = ts_write.generate_msgdef('std_msgs/msg/Float64', ros_version=2)
    rihs01_f64    = ts_write.hash_rihs01('std_msgs/msg/Float64')

    with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        conn_rtk_alt = writer.add_connection(
            _RTK_ALT_TOPIC, 'std_msgs/msg/Float64',
            msgdef=msgdef_f64, rihs01=rihs01_f64,
        )
        conn_diff = writer.add_connection(
            _DIFF_TOPIC, 'std_msgs/msg/Float64',
            msgdef=msgdef_f64, rihs01=rihs01_f64,
        )
        conn_alt = writer.add_connection(
            _ALTIMETER_TOPIC,
            alt_conn_info['msgtype'],
            msgdef=alt_conn_info['msgdef'],
            rihs01=alt_conn_info['rihs01'],
            serialization_format=alt_conn_info['serialization_format'],
            offered_qos_profiles=alt_conn_info['offered_qos_profiles'],
        )

        # write RTK altitude (relative)
        for ts, rel in zip(rtk_times, rtk_rel):
            msg = Float64(data=rel)
            raw = ts_write.serialize_cdr(msg, 'std_msgs/msg/Float64')
            writer.write(conn_rtk_alt, ts, raw)

        # write altimeter raw + diff at altimeter timestamps
        for ts, raw, diff in zip(alt_times, alt_raw, diffs):
            writer.write(conn_alt, ts, raw)
            msg = Float64(data=diff)
            raw_diff = ts_write.serialize_cdr(msg, 'std_msgs/msg/Float64')
            writer.write(conn_diff, ts, raw_diff)

    print(f'Wrote to {out_dir}')
    print(f'  {_RTK_ALT_TOPIC}  ({len(rtk_times)} msgs)')
    print(f'  {_ALTIMETER_TOPIC}  ({len(alt_times)} msgs)')
    print(f'  {_DIFF_TOPIC}  ({len(alt_times)} msgs)')
