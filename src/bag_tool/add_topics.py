"""Append topics from one bag into an existing bag in-place."""

from __future__ import annotations

import sys
from io import BytesIO, StringIO
from pathlib import Path

from ruamel.yaml import YAML

from rosbags.interfaces import MessageDefinitionFormat
from rosbags.rosbag2 import Reader
from rosbags.rosbag2.metadata import dump_qos_v9
from rosbags.rosbag2.storage_mcap import (
    Channel,
    ChunkInfo,
    McapReader,
    Schema,
    write_channel,
    write_record,
    write_schema,
    write_string,
    write_uint16,
    write_uint32,
    write_uint64,
)


def _find_mcap(path: Path) -> Path:
    if path.suffix == '.mcap':
        return path
    if path.is_dir():
        mcaps = list(path.glob('*.mcap'))
        if len(mcaps) != 1:
            print(f'ERROR: expected 1 .mcap in {path}, found {mcaps}', file=sys.stderr)
            sys.exit(1)
        return mcaps[0]
    print(f'ERROR: {path} is not a .mcap file or directory', file=sys.stderr)
    sys.exit(1)


def _qos_to_str(qos_profiles: list) -> str:
    dumped = dump_qos_v9(qos_profiles)
    if not isinstance(dumped, str):
        stream = StringIO()
        yaml = YAML(typ='safe')
        yaml.default_flow_style = False
        yaml.dump(dumped, stream)
        return stream.getvalue().strip()
    return dumped


def _reserialize_chunk_index(ci: ChunkInfo) -> BytesIO:
    rec = BytesIO()
    write_uint64(rec, ci.message_start_time)
    write_uint64(rec, ci.message_end_time)
    write_uint64(rec, ci.chunk_start_offset)
    write_uint64(rec, ci.chunk_length)
    offsets = sorted(ci.message_index_offsets.items())
    write_uint32(rec, len(offsets) * 10)
    for cid, offset in offsets:
        write_uint16(rec, cid)
        write_uint64(rec, offset)
    write_uint64(rec, ci.message_index_length)
    write_string(rec, ci.compression)
    write_uint64(rec, ci.compressed_size)
    write_uint64(rec, ci.uncompressed_size)
    return rec


def _update_metadata_yaml(
    dest_dir: Path,
    new_channels: list[Channel],
    channel_stats: dict[int, int],
    qos_by_channel: dict[int, list],
) -> None:
    yaml_path = dest_dir / 'metadata.yaml'
    if not yaml_path.exists():
        return
    yaml = YAML(typ='safe')
    dct = yaml.load(yaml_path.read_text())
    info = dct.get('rosbag2_bagfile_information', dct)
    for ch in new_channels:
        qos = qos_by_channel.get(ch.id, [])
        info['topics_with_message_count'].append({
            'message_count': channel_stats.get(ch.id, 0),
            'topic_metadata': {
                'name': ch.topic,
                'type': ch.schema,
                'serialization_format': ch.message_encoding,
                'offered_qos_profiles': _qos_to_str(qos),
                'type_description_hash': '',
            },
        })
    info['message_count'] = info.get('message_count', 0) + sum(channel_stats.values())
    out = StringIO()
    yaml.dump(dct, out)
    yaml_path.write_text(out.getvalue(), 'utf8')


def run(source_bag: str, dest_bag: str, topics: list[str]) -> None:
    """Append topics from source_bag into dest_bag in-place."""
    source_path = Path(source_bag)
    dest_path   = Path(dest_bag)
    source_mcap = _find_mcap(source_path)
    dest_mcap   = _find_mcap(dest_path)
    dest_is_dir = dest_path.is_dir()

    src_reader_path = source_path if source_path.is_dir() else source_mcap
    with Reader(src_reader_path) as r:
        src_conns_by_topic = {c.topic: c for c in r.connections}

    missing = [t for t in topics if t not in src_conns_by_topic]
    if missing:
        print(f'ERROR: topics not found in source bag: {missing}', file=sys.stderr)
        sys.exit(1)

    # Read dest MCAP state
    dest_reader = McapReader(dest_mcap)
    dest_reader.open()
    data_end          = dest_reader.data_end
    existing_schemas  = dict(dest_reader.schemas)
    existing_channels = dict(dest_reader.channels)
    existing_chunks   = list(dest_reader.chunks)
    existing_stats    = dest_reader.statistics
    dest_reader.close()

    duplicates = [t for t in topics if t in {ch.topic for ch in existing_channels.values()}]
    if duplicates:
        print(f'ERROR: topics already exist in dest bag: {duplicates}', file=sys.stderr)
        sys.exit(1)

    next_schema_id  = max(existing_schemas.keys(),  default=0) + 1
    next_channel_id = max(existing_channels.keys(), default=0) + 1
    msgtype_to_schema_id: dict[str, int] = {s.name: s.id for s in existing_schemas.values()}

    new_schemas:       list[Schema]    = []
    new_channels:      list[Channel]   = []
    new_chunk_indexes: list[BytesIO]   = []
    channel_stats:     dict[int, int]  = {}
    qos_by_channel:    dict[int, list] = {}

    overall_min_ts = existing_stats.start_time if existing_stats else 2**63 - 1
    overall_max_ts = existing_stats.end_time   if existing_stats else 0

    with open(dest_mcap, 'r+b') as bio:
        bio.seek(data_end)
        bio.truncate()

        with Reader(src_reader_path) as src_reader:
            for topic in topics:
                conn    = src_conns_by_topic[topic]
                msgtype = conn.msgtype

                # Schema: reuse existing or create new
                if msgtype not in msgtype_to_schema_id:
                    encoding = ('ros2msg' if conn.msgdef.format == MessageDefinitionFormat.MSG
                                else 'ros2idl')
                    schema = Schema(next_schema_id, msgtype, encoding, conn.msgdef.data)
                    new_schemas.append(schema)
                    msgtype_to_schema_id[msgtype] = next_schema_id
                    next_schema_id += 1

                # Channel
                meta = BytesIO()
                write_string(meta, 'offered_qos_profiles')
                write_string(meta, _qos_to_str(conn.ext.offered_qos_profiles))
                channel = Channel(
                    next_channel_id, msgtype, topic,
                    conn.ext.serialization_format, meta.getvalue(),
                )
                new_channels.append(channel)
                channel_stats[next_channel_id]  = 0
                qos_by_channel[next_channel_id] = conn.ext.offered_qos_profiles
                next_channel_id += 1

                # Build chunk content in memory
                all_schemas = list(existing_schemas.values()) + new_schemas
                chunk_bio   = BytesIO()
                write_schema(chunk_bio, next(s for s in all_schemas if s.name == msgtype))
                write_channel(chunk_bio, channel, all_schemas)

                chunk_min_ts = 2**63 - 1
                chunk_max_ts = 0
                msg_offsets: list[tuple[int, int]] = []

                for _, timestamp, rawdata in src_reader.messages(connections=[conn]):
                    msg_offsets.append((timestamp, chunk_bio.tell()))
                    chunk_min_ts = min(timestamp, chunk_min_ts)
                    chunk_max_ts = max(timestamp, chunk_max_ts)
                    overall_min_ts = min(timestamp, overall_min_ts)
                    overall_max_ts = max(timestamp, overall_max_ts)
                    channel_stats[channel.id] += 1

                    rec = BytesIO()
                    write_uint16(rec, channel.id)
                    write_uint32(rec, 0)           # sequence
                    write_uint64(rec, timestamp)   # log_time
                    write_uint64(rec, timestamp)   # publish_time
                    rec.write(rawdata)
                    write_record(chunk_bio, 0x05, rec)

                if not msg_offsets:
                    print(f'WARNING: topic {topic!r} has 0 messages in source, skipping')
                    continue

                raw_chunk = chunk_bio.getvalue()

                # Write Chunk(0x06)
                chunk_rec = BytesIO()
                write_uint64(chunk_rec, chunk_min_ts)
                write_uint64(chunk_rec, chunk_max_ts)
                write_uint64(chunk_rec, len(raw_chunk))   # uncompressed_size
                write_uint32(chunk_rec, 0)                # crc
                write_string(chunk_rec, '')               # no compression
                write_uint64(chunk_rec, len(raw_chunk))   # compressed_size
                chunk_rec.write(raw_chunk)
                chunk_start = bio.tell()
                write_record(bio, 0x06, chunk_rec)
                chunk_end = bio.tell()

                # Write MessageIndex(0x07)
                msg_index_offset = bio.tell()
                idx_rec = BytesIO()
                write_uint16(idx_rec, channel.id)
                write_uint32(idx_rec, len(msg_offsets) * 16)
                for ts, off in msg_offsets:
                    write_uint64(idx_rec, ts)
                    write_uint64(idx_rec, off)
                write_record(bio, 0x07, idx_rec)
                msg_index_end = bio.tell()

                # ChunkIndex record for this chunk
                ci_rec = BytesIO()
                write_uint64(ci_rec, chunk_min_ts)
                write_uint64(ci_rec, chunk_max_ts)
                write_uint64(ci_rec, chunk_start)
                write_uint64(ci_rec, chunk_end - chunk_start)
                write_uint32(ci_rec, 10)                          # one entry * 10 bytes
                write_uint16(ci_rec, channel.id)
                write_uint64(ci_rec, msg_index_offset)
                write_uint64(ci_rec, msg_index_end - chunk_end)   # message_index_length
                write_string(ci_rec, '')                          # no compression
                write_uint64(ci_rec, len(raw_chunk))              # compressed_size
                write_uint64(ci_rec, len(raw_chunk))              # uncompressed_size
                new_chunk_indexes.append(ci_rec)

        # DataEnd(0x0F) — required by MCAP spec to close the data section
        de = BytesIO()
        write_uint32(de, 0)
        write_record(bio, 0x0F, de)

        # Write summary section
        new_summary_start = bio.tell()

        all_schemas_combined  = list(existing_schemas.values()) + new_schemas
        all_channels_combined = list(existing_channels.values()) + new_channels

        schema_section_start = bio.tell()
        for s in all_schemas_combined:
            write_schema(bio, s)

        channel_section_start = bio.tell()
        for ch in all_channels_combined:
            write_channel(bio, ch, all_schemas_combined)

        chunk_section_start = bio.tell()
        for ci in existing_chunks:
            write_record(bio, 0x08, _reserialize_chunk_index(ci))
        for ci_rec in new_chunk_indexes:
            write_record(bio, 0x08, ci_rec)

        stats_section_start = bio.tell()
        old_ch_stats     = existing_stats.channel_message_counts if existing_stats else {}
        combined_ch_stats = {**old_ch_stats, **channel_stats}
        stats_rec = BytesIO()
        write_uint64(stats_rec, sum(combined_ch_stats.values()))
        write_uint16(stats_rec, len(all_schemas_combined))
        write_uint32(stats_rec, len(all_channels_combined))
        write_uint32(stats_rec, 0)                                                      # attachments
        write_uint32(stats_rec, 0)                                                      # metadata
        write_uint32(stats_rec, len(existing_chunks) + len(new_chunk_indexes))
        write_uint64(stats_rec, overall_min_ts)
        write_uint64(stats_rec, overall_max_ts)
        write_uint32(stats_rec, len(combined_ch_stats) * 10)
        for cid, count in sorted(combined_ch_stats.items()):
            write_uint16(stats_rec, cid)
            write_uint64(stats_rec, count)
        write_record(bio, 0x0B, stats_rec)

        summary_offset_start = bio.tell()

        if schema_section_start != channel_section_start:
            rec = BytesIO()
            rec.write(b'\x03')
            write_uint64(rec, schema_section_start)
            write_uint64(rec, channel_section_start - schema_section_start)
            write_record(bio, 0x0E, rec)

        if channel_section_start != chunk_section_start:
            rec = BytesIO()
            rec.write(b'\x04')
            write_uint64(rec, channel_section_start)
            write_uint64(rec, chunk_section_start - channel_section_start)
            write_record(bio, 0x0E, rec)

        if chunk_section_start != stats_section_start:
            rec = BytesIO()
            rec.write(b'\x08')
            write_uint64(rec, chunk_section_start)
            write_uint64(rec, stats_section_start - chunk_section_start)
            write_record(bio, 0x0E, rec)

        rec = BytesIO()
        rec.write(b'\x0b')
        write_uint64(rec, stats_section_start)
        write_uint64(rec, summary_offset_start - stats_section_start)
        write_record(bio, 0x0E, rec)

        # Footer(0x02)
        foot = BytesIO()
        write_uint64(foot, new_summary_start)
        write_uint64(foot, summary_offset_start)
        write_uint32(foot, 0)   # crc
        write_record(bio, 0x02, foot)
        bio.write(b'\x89MCAP\x30\r\n')

    if dest_is_dir:
        _update_metadata_yaml(dest_path, new_channels, channel_stats, qos_by_channel)

    total_new = sum(channel_stats.values())
    print(f'Appended {total_new} messages ({len(new_channels)} topics) to {dest_path}')
