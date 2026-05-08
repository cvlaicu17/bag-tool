"""filter-imu: apply a low-pass filter to the IMU topic in a ROS2 bag."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import numpy as np
from scipy.signal import butter, sosfilt, sosfilt_zi
from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore, Stores


_DEFAULT_CUTOFF_HZ   = 80.0
_DEFAULT_ORDER       = 4
_DEFAULT_IMU_TOPIC   = "/imu/data_raw"
_NOMINAL_SAMPLE_RATE = 400.0


def _rihs01(msgdef_str: str) -> str:
    digest = hashlib.sha256(msgdef_str.encode()).hexdigest()
    return f'RIHS01_{digest}'


class _AxisFilter:
    def __init__(self, sos: np.ndarray):
        self._sos = sos
        self._zi  = sosfilt_zi(sos)

    def push(self, value: float) -> float:
        y, self._zi = sosfilt(self._sos, [value], zi=self._zi)
        return float(y[0])


def run(
    input_bag: str,
    output_bag: str,
    stores: Stores,
    imu_topic: str = _DEFAULT_IMU_TOPIC,
    cutoff: float = _DEFAULT_CUTOFF_HZ,
    order: int = _DEFAULT_ORDER,
) -> None:
    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    typestore = get_typestore(stores)

    sos = butter(order, cutoff, btype='low', fs=_NOMINAL_SAMPLE_RATE, output='sos')
    filters = {ax: _AxisFilter(sos) for ax in ('ax', 'ay', 'az', 'gx', 'gy', 'gz')}

    connections_by_topic: dict[str, object] = {}
    n_total = n_imu = 0

    with Reader(reader_path) as reader:
        imu_conns = {c.topic for c in reader.connections if c.topic == imu_topic}
        if not imu_conns:
            print(f'ERROR: topic {imu_topic!r} not found in bag', file=sys.stderr)
            sys.exit(1)

        imu_msgtype = next(
            c.msgtype for c in reader.connections if c.topic == imu_topic
        )

        with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            for connection, timestamp, rawdata in reader.messages():
                n_total += 1

                if connection.topic == imu_topic:
                    n_imu += 1
                    msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                    msg.linear_acceleration.x = filters['ax'].push(msg.linear_acceleration.x)
                    msg.linear_acceleration.y = filters['ay'].push(msg.linear_acceleration.y)
                    msg.linear_acceleration.z = filters['az'].push(msg.linear_acceleration.z)
                    msg.angular_velocity.x    = filters['gx'].push(msg.angular_velocity.x)
                    msg.angular_velocity.y    = filters['gy'].push(msg.angular_velocity.y)
                    msg.angular_velocity.z    = filters['gz'].push(msg.angular_velocity.z)
                    rawdata = typestore.serialize_cdr(msg, connection.msgtype)

                if connection.topic not in connections_by_topic:
                    msgdef_str = connection.msgdef[1] if connection.msgdef else ''
                    rihs01 = connection.digest or _rihs01(msgdef_str)
                    connections_by_topic[connection.topic] = writer.add_connection(
                        connection.topic,
                        connection.msgtype,
                        msgdef=msgdef_str,
                        rihs01=rihs01,
                        serialization_format=connection.ext.serialization_format,
                        offered_qos_profiles=connection.ext.offered_qos_profiles,
                    )

                writer.write(connections_by_topic[connection.topic], timestamp, rawdata)

                if n_total % 10_000 == 0:
                    print(f'  {n_total:,} messages processed...', end='\r', flush=True)

    print(f'  {n_total:,} messages total, {n_imu:,} IMU messages filtered.')
    print(f'Wrote output to {out_dir}')
