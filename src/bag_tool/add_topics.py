"""Append topics from one bag into a new output bag that also contains all dest topics."""

from __future__ import annotations

import re
import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin

_ROS2_PRIMITIVES = frozenset([
    'bool', 'byte', 'char',
    'float32', 'float64',
    'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64',
    'string', 'wstring',
])


def _normalize_msgdef(msgdef: str) -> str:
    """Strip default-value annotations so merged bags have consistent nested-type definitions.

    rosbags typestore emits IDL with @default(...) annotations; original ROS2 bags use
    plain MSG fields with no defaults.  Foxglove parses both forms and reports a conflict
    when the same nested type appears with different inline definitions across schemas.
    """
    if '\nIDL: ' in msgdef[:200] or msgdef.lstrip().startswith('IDL: '):
        # IDL format — remove @default(...) annotations (may be on their own line or inline)
        return re.sub(r'\s*@default\s*\([^)]*\)', '', msgdef)

    # ros2msg format — strip trailing default values from primitive field declarations.
    # Fields with defaults look like:  "float64 w 1"  (type, name, value)
    # Constants look like:             "float64 MY_K=3.14"  — leave those alone.
    lines = []
    for line in msgdef.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            lines.append(line)
            continue
        field_part = stripped.split('#', 1)[0]
        tokens = field_part.split()
        if len(tokens) == 3 and '=' not in tokens[0] and tokens[0] in _ROS2_PRIMITIVES:
            indent = len(line) - len(line.lstrip())
            lines.append(' ' * indent + f'{tokens[0]} {tokens[1]}')
        else:
            lines.append(line)
    return '\n'.join(lines)


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
