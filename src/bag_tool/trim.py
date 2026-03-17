"""Trim a ROS2 bag to a time window."""

from __future__ import annotations
import hashlib
import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin


def _rihs01(msgdef_str: str) -> str:
    """Compute a RIHS01-style hash from the raw msgdef string as a fallback."""
    digest = hashlib.sha256(msgdef_str.encode()).hexdigest()
    return f'RIHS01_{digest}'


def run(input_bag: str, output_bag: str, start_s: float | None, end_s: float | None) -> None:
    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    with Reader(reader_path) as reader:
        bag_start_ns = reader.start_time  # nanoseconds

        start_ns = bag_start_ns + int(start_s * 1e9) if start_s is not None else None
        end_ns   = bag_start_ns + int(end_s   * 1e9) if end_s   is not None else None

        start_fmt = f'{start_s:.3f}s' if start_s is not None else 'beginning'
        end_fmt   = f'{end_s:.3f}s'   if end_s   is not None else 'end'
        print(f'Trimming: {start_fmt} → {end_fmt} (relative to bag start)')

        connections_by_topic: dict[str, object] = {}
        count = 0

        with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            for connection, timestamp, rawdata in reader.messages(
                start=start_ns,
                stop=end_ns,
            ):
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
                count += 1

    print(f'Wrote {count} messages to {out_dir}')
