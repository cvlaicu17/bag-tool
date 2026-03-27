"""Append topics from one bag into a new output bag that also contains all dest topics."""

from __future__ import annotations

import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin


_QUAT_DEFAULTS = {
    'float64 x': 'float64 x 0',
    'float64 y': 'float64 y 0',
    'float64 z': 'float64 z 0',
    'float64 w': 'float64 w 1',
}


def _normalize_msgdef(msgdef: str) -> str:
    """Ensure geometry_msgs/Quaternion fields carry standard defaults (x=0,y=0,z=0,w=1).

    Rosbags typestore-generated schemas omit field defaults; DJI and VIO bags include
    them.  Adding them here keeps all schemas in a merged bag consistent and avoids
    Foxglove cross-bag schema conflicts when opening alongside the original recording.
    Fields that already carry a default are left unchanged.
    """
    lines = msgdef.splitlines()
    result = []
    in_quat = False
    for line in lines:
        stripped = line.strip()
        if 'MSG: geometry_msgs/Quaternion' in stripped:
            in_quat = True
            result.append(line)
            continue
        if in_quat:
            if stripped.startswith('MSG:') or stripped.startswith('====='):
                in_quat = False
            elif stripped in _QUAT_DEFAULTS:
                indent = len(line) - len(line.lstrip())
                result.append(' ' * indent + _QUAT_DEFAULTS[stripped])
                continue
        result.append(line)
    return '\n'.join(result)


def _reader_path(path: Path) -> Path:
    """Return the path suitable for rosbags Reader (directory or .mcap file)."""
    if path.is_dir():
        return path
    if path.suffix == '.mcap':
        return path
    mcaps = list(path.glob('*.mcap'))
    if len(mcaps) != 1:
        print(f'ERROR: expected 1 .mcap in {path}, found {mcaps}', file=sys.stderr)
        sys.exit(1)
    return mcaps[0]


def _add_conn(writer: object, conn: object) -> object:
    msgdef = conn.msgdef.data if isinstance(conn.msgdef.data, str) else conn.msgdef.data.decode()
    return writer.add_connection(
        conn.topic, conn.msgtype,
        msgdef=_normalize_msgdef(msgdef),
        rihs01=conn.digest or 'rihs01:' + '0' * 64,
        serialization_format=conn.ext.serialization_format,
        offered_qos_profiles=conn.ext.offered_qos_profiles,
    )


def run(source_bag: str, dest_bag: str, topics: list[str]) -> None:
    """Write a new bag containing all topics from dest_bag plus selected topics from source_bag."""
    source_path = Path(source_bag)
    dest_path   = Path(dest_bag)

    src_rpath  = _reader_path(source_path)
    dest_rpath = _reader_path(dest_path)

    # Keep both Readers open through the Writer block so each bag is only opened once.
    with Reader(src_rpath) as src_reader:
        src_conns_by_topic = {c.topic: c for c in src_reader.connections}

        if not topics:
            topics = list(src_conns_by_topic.keys())
            print(f'No topics specified — using all {len(topics)} topics from source bag')

        missing = [t for t in topics if t not in src_conns_by_topic]
        if missing:
            print(f'ERROR: topics not found in source bag: {missing}', file=sys.stderr)
            sys.exit(1)

        src_topic_conns = [src_conns_by_topic[t] for t in topics]

        out_path = dest_path.parent / (dest_path.stem + '_copy')
        if out_path.exists():
            print(f'ERROR: output path already exists: {out_path}', file=sys.stderr)
            sys.exit(1)

        with Reader(dest_rpath) as dest_reader:
            dest_conns = list(dest_reader.connections)

            dest_topics = {c.topic for c in dest_conns}
            duplicates  = [t for t in topics if t in dest_topics]
            if duplicates:
                print(f'ERROR: topics already exist in dest bag: {duplicates}', file=sys.stderr)
                sys.exit(1)

            with Writer(out_path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
                dest_conn_map: dict[int, object] = {
                    conn.id: _add_conn(writer, conn) for conn in dest_conns
                }
                src_conn_map: dict[int, object] = {
                    conn.id: _add_conn(writer, conn) for conn in src_topic_conns
                }

                for conn, ts, rawdata in dest_reader.messages():
                    writer.write(dest_conn_map[conn.id], ts, rawdata)

                for conn, ts, rawdata in src_reader.messages(connections=src_topic_conns):
                    writer.write(src_conn_map[conn.id], ts, rawdata)

    new_msg_count = sum(c.msgcount for c in src_conn_map.values())
    print(f'Appended {new_msg_count} messages ({len(topics)} topics) → {out_path}')
