"""
Fix sensor_msgs/msg/Range CDR encoding from Humble to Jazzy.
Humble Range has no 'variance' field; Jazzy added it.
This appends variance=0.0 (4 bytes) to each Range message.
"""

from __future__ import annotations

import struct
import sys

import rosbag2_py
from rclpy.serialization import deserialize_message, serialize_message
from sensor_msgs.msg import Range


def run(input_bag: str, output_bag: str) -> None:
    reader = rosbag2_py.SequentialReader()
    reader.open(
        rosbag2_py.StorageOptions(uri=input_bag, storage_id='mcap'),
        rosbag2_py.ConverterOptions('cdr', 'cdr'),
    )

    topics = reader.get_all_topics_and_types()

    writer = rosbag2_py.SequentialWriter()
    writer.open(
        rosbag2_py.StorageOptions(uri=output_bag, storage_id='mcap'),
        rosbag2_py.ConverterOptions('cdr', 'cdr'),
    )

    for t in topics:
        writer.create_topic(t)

    range_count = 0
    total_count = 0
    while reader.has_next():
        topic, data, timestamp = reader.read_next()
        total_count += 1

        if topic == '/altimeter/range':
            data = data + struct.pack('<f', 0.0)
            range_count += 1

        writer.write(topic, data, timestamp)

        if total_count % 50000 == 0:
            print(f'  processed {total_count} messages ({range_count} range)...')

    print(f'Done: {total_count} total, {range_count} range messages fixed')

    # Verify
    reader2 = rosbag2_py.SequentialReader()
    reader2.open(
        rosbag2_py.StorageOptions(uri=output_bag, storage_id='mcap'),
        rosbag2_py.ConverterOptions('cdr', 'cdr'),
    )
    verified = 0
    while reader2.has_next():
        topic, data, t = reader2.read_next()
        if topic == '/altimeter/range':
            msg = deserialize_message(data, Range)
            if verified < 3:
                print(f'  verified: range={msg.range:.3f} frame={msg.header.frame_id}')
            verified += 1
            if verified >= 3:
                break
    print(f'Verification: {verified} Range messages deserialized OK')
