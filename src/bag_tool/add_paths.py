"""add-paths: produce a sibling bag with accumulating nav_msgs/Path topics.

Workflow target: shrunken bags (poses only, no paths). For each pose-like topic,
emit a Path message at the same timestamp as each pose, containing every pose
up to that point.

Topic discovery is by message type — any
    geometry_msgs/msg/PoseWithCovarianceStamped
    geometry_msgs/msg/PoseStamped
becomes a candidate. The path topic name is derived:
    /foo/pose     → /foo/path
    /foo/aligned  → /foo/aligned_path
    /foo          → /foo_path

Performance: a PoseStamped element inside a Path has the same CDR layout as the
header+pose prefix of a PoseWithCovarianceStamped (modulo the covariance tail).
So the inner loop just slices `raw[4 : 4+elem_size]` and concatenates into a
per-topic bytearray — no per-message deserialization. The path header is
likewise assembled by hand. Matches the technique used in processor.py's
write_alignment_topics.

Foxglove visibility: /tf_static is passed through so the output bag is viewable
standalone. Each path's header.frame_id is set to the same frame_id as the
source pose topic (read from the first message we see on that topic).
"""

from __future__ import annotations

import shutil
import struct
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore

from bag_tool.add_topics import _add_conn, _normalize_msgdef


POSE_TYPES = frozenset({
    'geometry_msgs/msg/PoseWithCovarianceStamped',
    'geometry_msgs/msg/PoseStamped',
})

_ENCAP = b'\x00\x01\x00\x00'


def derive_path_topic(pose_topic: str) -> str:
    """Map a pose topic to its corresponding path topic name."""
    if pose_topic.endswith('/pose'):
        return pose_topic[:-5] + '/path'
    return pose_topic + '_path'


def _cdr_string(s: str) -> bytes:
    """CDR-encode a string: uint32 length + null-terminated UTF-8 bytes."""
    b = s.encode('utf-8') + b'\x00'
    return struct.pack('<I', len(b)) + b


def _path_header_fid_pad(frame_id: str) -> bytes:
    """CDR-encoded frame_id + alignment pad bytes that go between Path.header.stamp
    and the uint32 poses[] count. After encap(4)+stamp(8), pad string end to 4."""
    fid_b = _cdr_string(frame_id)
    pad = (-(12 + len(fid_b))) & 3
    return fid_b + b'\x00' * pad


def _pose_elem_size(frame_id: str) -> int:
    """Size in bytes of one PoseStamped element inside a Path's poses[] for the
    given frame_id. Layout: stamp(8) + cdr_str(fid) + pad-to-8 + pos(24) + quat(32)."""
    fid_b = _cdr_string(frame_id)
    pad = (-(8 + len(fid_b))) & 7  # next field is double, 8-aligned
    return 8 + len(fid_b) + pad + 24 + 32


def _make_path_header_fn(frame_id: str):
    """Return a closure: (stamp_ns, n_poses) -> bytes producing the Path header
    bytes up to and including the poses[] count, ready to be followed by the
    accumulated pose-element buffer."""
    fid_pad = _path_header_fid_pad(frame_id)
    def header_fn(stamp_ns: int, n_poses: int) -> bytes:
        sec, nsec = divmod(stamp_ns, 10 ** 9)
        return _ENCAP + struct.pack('<II', sec, nsec) + fid_pad + struct.pack('<I', n_poses)
    return header_fn


def _extract_frame_id(raw: bytes) -> str:
    """Read header.frame_id from a CDR-serialized message starting with std_msgs/Header."""
    fid_len = struct.unpack_from('<I', raw, 12)[0]
    return raw[16:16 + fid_len - 1].decode('utf-8', errors='replace')


def _extract_stamp_ns(raw: bytes) -> int:
    """Read header.stamp from a CDR-serialized message starting with std_msgs/Header."""
    sec, nsec = struct.unpack_from('<II', raw, 4)
    return sec * 10**9 + nsec


def run(input_bag: str, stores_enum) -> None:
    """Read pose-like topics from input_bag, write accumulating paths to a sibling bag."""
    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path
    out_dir = input_path.parent / (input_path.stem + '_paths')
    if out_dir.exists():
        shutil.rmtree(out_dir)
        print(f'Removed existing output: {out_dir}')

    typestore = get_typestore(stores_enum)
    path_msgdef, path_rihs01 = typestore.generate_msgdef('nav_msgs/msg/Path')

    with Reader(reader_path) as reader:
        pose_conns = [c for c in reader.connections if c.msgtype in POSE_TYPES]
        tf_static_conn = next((c for c in reader.connections if c.topic == '/tf_static'), None)

        if not pose_conns:
            print(f'No pose topics found in {reader_path}; nothing to do.')
            return

        print(f'Discovered {len(pose_conns)} pose topic candidate(s):')
        for c in pose_conns:
            print(f'  {c.topic} ({c.msgtype})')

        with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            # Passthrough /tf_static for Foxglove standalone viewing.
            if tf_static_conn is not None:
                tf_out = _add_conn(writer, tf_static_conn)
                for c, bag_ts, raw in reader.messages(connections=[tf_static_conn]):
                    writer.write(tf_out, bag_ts, raw)
                print('Copied /tf_static for frame visibility')

            # Per-topic state built lazily on first message of each topic, so we
            # learn the frame_id without a separate discovery pass.
            state: dict[str, dict] = {}

            for conn, bag_ts, raw in reader.messages(connections=pose_conns):
                topic = conn.topic
                st = state.get(topic)
                if st is None:
                    fid = _extract_frame_id(raw)
                    target = derive_path_topic(topic)
                    elem = _pose_elem_size(fid)
                    conn_out = writer.add_connection(
                        target, 'nav_msgs/msg/Path',
                        msgdef=_normalize_msgdef(path_msgdef),
                        rihs01=path_rihs01,
                        serialization_format='cdr',
                        offered_qos_profiles='',
                    )
                    st = {
                        'target': target,
                        'header_fn': _make_path_header_fn(fid),
                        'elem_slice_end': 4 + elem,
                        'count': 0,
                        'buf': bytearray(),
                        'conn_out': conn_out,
                    }
                    state[topic] = st
                    print(f'  {topic} (frame_id={fid!r}) → {target}')

                # Slice the source's stamp+fid+pos+quat block (skip encap, stop
                # before optional covariance) and append to the path's poses[] buffer.
                st['buf'].extend(raw[4:st['elem_slice_end']])
                st['count'] += 1
                stamp_ns = _extract_stamp_ns(raw)
                writer.write(
                    st['conn_out'], bag_ts,
                    st['header_fn'](stamp_ns, st['count']) + bytes(st['buf']),
                )

    print(f'\nOutput: {out_dir}')
    for topic, st in state.items():
        print(f'  {st["count"]} path messages on {st["target"]}')
