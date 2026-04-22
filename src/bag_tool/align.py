"""align: RTK-VIO alignment written back into the same bag's folder."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore

from bag_tool.add_topics import _add_conn, _reader_path
from bag_tool.processor import COMPUTED_TOPICS, compute_alignment, write_alignment_topics


def _detect_aruco_for_align(
    input_bag: str,
    vio_topic: str,
    stores_enum,
    cam_path: Path,
    cam_to_vio_ts_offset: int,
) -> float | None:
    """Load posimus from input_bag, then run ArUco detection in cam_path.

    cam_to_vio_ts_offset: already-computed clock offset (cam bag_ts - offset = VIO bag_ts).
    """
    from bag_tool.aruco_align import detect_aruco_north

    typestore = get_typestore(stores_enum)
    input_path = Path(input_bag)
    vio_reader_path = input_path.parent if input_path.is_file() else input_path

    posimus = []
    with Reader(vio_reader_path) as reader:
        conns = [c for c in reader.connections if c.topic == vio_topic]
        for conn, ts, rawdata in reader.messages(connections=conns):
            posimus.append((ts, typestore.deserialize_cdr(rawdata, conn.msgtype)))
    posimus.sort(key=lambda x: x[0])

    return detect_aruco_north(cam_path, posimus, typestore,
                              cam_to_vio_ts_offset=cam_to_vio_ts_offset)


def run(
    input_bag: str,
    vio_topic: str,
    stores_enum,
    ref_bag: str | None = None,
    quick: bool = False,
    rte_window: float = 1.0,
    eval_mode: bool = False,
    manual: bool = False,
) -> None:
    """Compute RTK-VIO alignment from input_bag and write a new aligned bag next to it."""
    input_path = Path(input_bag)

    # Read ref_bag metadata upfront — needed for both ts_offset and ArUco clock correction.
    ts_offset = 0
    ref_topics: frozenset[str] = frozenset()
    cam_path = _reader_path(input_path)  # fallback: camera in VIO bag itself
    cam_to_vio_ts_offset = 0

    if ref_bag is not None:
        ref_rpath = _reader_path(Path(ref_bag))
        input_reader_path_tmp = input_path.parent if input_path.is_file() else input_path
        with Reader(input_reader_path_tmp) as vio_r:
            input_start = vio_r.start_time
            input_end   = vio_r.start_time + vio_r.duration
        with Reader(ref_rpath) as ref_reader:
            ref_start  = ref_reader.start_time
            ref_end    = ref_reader.start_time + ref_reader.duration
            ref_topics = frozenset(c.topic for c in ref_reader.connections)
        if ref_end < input_start or ref_start > input_end:
            ts_offset = ref_start - input_start
            cam_to_vio_ts_offset = ts_offset
            print(f'Timestamp offset: {ts_offset:+d} ns applied '
                  f'(input clock shifted to ref bag clock)')
        cam_path = ref_rpath

    # ArUco north alignment — uses the same clock offset already computed above.
    aruco_yaw_rad = None
    if not manual:
        aruco_yaw_rad = _detect_aruco_for_align(
            input_bag, vio_topic, stores_enum,
            cam_path=cam_path,
            cam_to_vio_ts_offset=cam_to_vio_ts_offset,
        )

    out_poses, out_aligned, posimus, typestore, input_reader_path, input_start, input_end, diag_tracking = \
        compute_alignment(input_bag, vio_topic, stores_enum, aruco_yaw_rad=aruco_yaw_rad)

    out_path = input_path.parent / (input_path.stem + '_aligned')
    if out_path.exists():
        shutil.rmtree(out_path)
        print(f'Removed existing output: {out_path}')

    with Reader(input_reader_path) as input_reader:
        with Writer(out_path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            metrics = write_alignment_topics(writer, typestore, out_poses, out_aligned, posimus, quick,
                                             ts_offset=ts_offset,
                                             rte_window_ns=int(rte_window * 1e9),
                                             eval_mode=eval_mode,
                                             diag_tracking=diag_tracking)

            if not eval_mode and not quick:
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

    if eval_mode and metrics:
        json_path = out_path.parent / (out_path.name + '.json')
        json_path.write_text(json.dumps(metrics, indent=2))
        print(f'Eval metrics written: {json_path}')

    # Hard-link the .mcap file next to ref_bag for easy access.
    if ref_bag is not None:
        mcap_file = out_path / f'{out_path.name}.mcap'
        ref_path = Path(ref_bag)
        link_path = ref_path.parent / mcap_file.name
        if link_path.exists():
            link_path.unlink()
        link_path.hardlink_to(mcap_file)
        print(f'Hard link created: {link_path}')

        if eval_mode and metrics:
            ref_json_path = link_path.with_suffix('.json')
            ref_json_path.write_text(json.dumps(metrics, indent=2))
            print(f'Eval metrics written (ref): {ref_json_path}')
