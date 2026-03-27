"""align: RTK-VIO alignment + dest bag passthrough in a single Writer pass."""

from __future__ import annotations

import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin

from bag_tool.add_topics import _add_conn, _reader_path
from bag_tool.processor import COMPUTED_TOPICS, compute_alignment, write_alignment_topics


def run(
    input_bag: str,
    dest_bag: str,
    vio_topic: str,
    stores_enum,
    quick: bool = False,
) -> None:
    """Compute RTK-VIO alignment from input_bag and merge everything into one output bag.

    Output: <dest_bag>_aligned/
    - 5 computed alignment topics
    - all topics from dest_bag (raw passthrough)
    - remaining topics from input_bag not already in dest_bag (raw passthrough)
    No intermediate bag is written.
    """
    out_poses, out_aligned, posimus, typestore, input_reader_path, input_start, input_end = \
        compute_alignment(input_bag, vio_topic, stores_enum)

    dest_path = Path(dest_bag)
    out_path  = dest_path.parent / (dest_path.stem + '_aligned')
    if out_path.exists():
        print(f'ERROR: output path {out_path} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    dest_rpath = _reader_path(dest_path)

    # Keep dest Reader open through the Writer block — single index parse, single pass.
    with Reader(dest_rpath) as dest_reader:
        dest_conns = list(dest_reader.connections)
        dest_topics = {c.topic for c in dest_conns}

        # Detect timestamp offset: if dest_bag and input_bag timestamps don't overlap,
        # shift input_bag topics to dest_bag's clock so the output aligns with the original recording.
        # Common case: VIO bags use Unix wall time; DJI raw bags use boot-clock (seconds from boot).
        ts_offset = 0
        dest_end = dest_reader.start_time + dest_reader.duration
        if dest_end < input_start or dest_reader.start_time > input_end:
            ts_offset = dest_reader.start_time - input_start
            print(f'Timestamp offset: {ts_offset:+d} ns applied to input bag topics '
                  f'(input clock shifted to dest bag clock)')

        with Writer(out_path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            write_alignment_topics(writer, typestore, out_poses, out_aligned, posimus, quick,
                                   ts_offset=ts_offset)

            # Passthrough: all dest_bag topics (skip any that clash with computed)
            dest_conn_map: dict[int, object] = {
                conn.id: _add_conn(writer, conn)
                for conn in dest_conns
                if conn.topic not in COMPUTED_TOPICS
            }
            if dest_conn_map:
                passthrough_dest = [c for c in dest_conns if c.id in dest_conn_map]
                for conn, ts, rawdata in dest_reader.messages(connections=passthrough_dest):
                    writer.write(dest_conn_map[conn.id], ts, rawdata)

            # Passthrough: remaining input_bag topics not already registered
            already_registered = COMPUTED_TOPICS | dest_topics
            with Reader(input_reader_path) as input_reader:
                input_conn_map: dict[int, object] = {}
                for c in input_reader.connections:
                    if c.topic not in already_registered:
                        input_conn_map[c.id] = _add_conn(writer, c)
                if input_conn_map:
                    passthrough = [c for c in input_reader.connections if c.id in input_conn_map]
                    for c, ts, rawdata in input_reader.messages(connections=passthrough):
                        writer.write(input_conn_map[c.id], ts + ts_offset, rawdata)

    print(f'Output written to: {out_path}')
