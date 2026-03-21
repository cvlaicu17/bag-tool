"""Re-serialise a bag from Jazzy to Humble message definitions."""

from __future__ import annotations
import sys
from pathlib import Path

from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore, Stores
from rosbags.typesys.store import Typestore

from bag_tool.trim import _rihs01


def _add_conn(writer: Writer, conn, typestore: Typestore):
    """Register a connection using Humble message definitions where available."""
    msgtype = conn.msgtype
    if msgtype in typestore.fielddefs:
        msgdef, _ = typestore.generate_msgdef(msgtype, ros_version=2)
        rihs01   = typestore.hash_rihs01(msgtype)
    else:
        msgdef_str = conn.msgdef[1] if conn.msgdef else ''
        msgdef  = msgdef_str
        rihs01  = conn.digest or _rihs01(msgdef_str)

    return writer.add_connection(
        conn.topic,
        msgtype,
        msgdef=msgdef,
        rihs01=rihs01,
        serialization_format=conn.ext.serialization_format,
        offered_qos_profiles=conn.ext.offered_qos_profiles,
    )


def run(input_bag: str, output_bag: str) -> None:
    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    ts_src  = get_typestore(Stores.ROS2_JAZZY)
    ts_dst  = get_typestore(Stores.ROS2_HUMBLE)

    # Pre-compute which message types need re-serialisation (definition differs between stores)
    needs_reserialise: dict[str, bool] = {}

    def _reserialise_needed(msgtype: str) -> bool:
        if msgtype not in needs_reserialise:
            if msgtype not in ts_src.fielddefs or msgtype not in ts_dst.fielddefs:
                needs_reserialise[msgtype] = False
            else:
                src_fields = ts_src.fielddefs[msgtype][1]
                dst_fields = ts_dst.fielddefs[msgtype][1]
                needs_reserialise[msgtype] = src_fields != dst_fields
        return needs_reserialise[msgtype]

    conn_map: dict[int, object] = {}
    counts: dict[str, int] = {}
    reserialised: set[str] = set()

    with Reader(reader_path) as reader:
        with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            for conn, ts, raw in reader.messages():
                if conn.id not in conn_map:
                    conn_map[conn.id] = _add_conn(writer, conn, ts_dst)

                out_conn = conn_map[conn.id]
                msgtype  = conn.msgtype

                if _reserialise_needed(msgtype):
                    reserialised.add(msgtype)
                    try:
                        msg     = ts_src.deserialize_cdr(raw, msgtype)
                        raw_out = ts_dst.serialize_cdr(msg, msgtype)
                    except Exception:
                        raw_out = raw  # fall back to raw on error
                else:
                    raw_out = raw

                writer.write(out_conn, ts, raw_out)
                counts[msgtype] = counts.get(msgtype, 0) + 1

    total = sum(counts.values())
    print(f'Wrote {total} messages to {out_dir}')
    if reserialised:
        print(f'Re-serialised types (Jazzy → Humble):')
        for t in sorted(reserialised):
            print(f'  {t}  ({counts[t]} msgs)')
    else:
        print('No type differences found — all messages passed through raw.')
