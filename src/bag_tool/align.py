"""align: RTK-VIO alignment written back into the same bag's folder."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin

from bag_tool.add_topics import _add_conn, _reader_path
from bag_tool.processor import COMPUTED_TOPICS, compute_alignment, write_alignment_topics


def run(
    input_bag: str,
    vio_topic: str,
    stores_enum,
    ref_bag: str | None = None,
    quick: bool = False,
    rte_window: float = 1.0,
    eval_mode: bool = False,
) -> None:
    """Compute RTK-VIO alignment from input_bag and write a new aligned bag next to it.

    Output: <input_bag_parent>/<input_bag_stem>_aligned/
    - 5 computed alignment topics
    - all remaining topics from input_bag (raw passthrough, excluding any topic
      already present in ref_bag to avoid schema conflicts when both are open)

    If ref_bag is provided and its clock doesn't overlap with input_bag, all output
    timestamps are shifted so the aligned bag appears at the same timeline position
    as ref_bag (e.g. original DJI boot-clock recording vs. VIO Unix wall-clock).
    No intermediate bag is written.
    """
    out_poses, out_aligned, posimus, typestore, input_reader_path, input_start, input_end = \
        compute_alignment(input_bag, vio_topic, stores_enum)

    input_path = Path(input_bag)
    out_path   = input_path.parent / (input_path.stem + '_aligned')
    if out_path.exists():
        shutil.rmtree(out_path)
        print(f'Removed existing output: {out_path}')

    # Optional timestamp alignment: shift all output to ref_bag's clock frame.
    # Also collect ref_bag topics so we don't duplicate them in the passthrough.
    ts_offset = 0
    ref_topics: frozenset[str] = frozenset()
    if ref_bag is not None:
        ref_rpath = _reader_path(Path(ref_bag))
        with Reader(ref_rpath) as ref_reader:
            ref_start  = ref_reader.start_time
            ref_end    = ref_reader.start_time + ref_reader.duration
            ref_topics = frozenset(c.topic for c in ref_reader.connections)
        if ref_end < input_start or ref_start > input_end:
            ts_offset = ref_start - input_start
            print(f'Timestamp offset: {ts_offset:+d} ns applied '
                  f'(input clock shifted to ref bag clock)')
    with Reader(input_reader_path) as input_reader:
        with Writer(out_path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            write_alignment_topics(writer, typestore, out_poses, out_aligned, posimus, quick,
                                   ts_offset=ts_offset,
                                   rte_window_ns=int(rte_window * 1e9),
                                   eval_mode=eval_mode)

            if not eval_mode:
                skip = COMPUTED_TOPICS | ref_topics
                conn_map: dict[int, object] = {
                    c.id: _add_conn(writer, c)
                    for c in input_reader.connections
                    if c.topic not in skip
                }
                if conn_map:
                    passthrough = [c for c in input_reader.connections if c.id in conn_map]
                    for c, ts, rawdata in input_reader.messages(connections=passthrough):
                        writer.write(conn_map[c.id], ts + ts_offset, rawdata)

    print(f'Output written to: {out_path}')

    # Hard-link the .mcap file next to ref_bag for easy access.
    if ref_bag is not None:
        mcap_file = out_path / f'{out_path.name}.mcap'
        ref_path = Path(ref_bag)
        link_path = ref_path.parent / mcap_file.name
        if link_path.exists():
            link_path.unlink()
        link_path.hardlink_to(mcap_file)
        print(f'Hard link created: {link_path}')
