"""Helpers for reading back what a test wrote to a real broker."""

import time

from confluent_kafka import Consumer, Message, TopicPartition

READ_TIMEOUT_S = 10.0


def read_at(bootstrap: str, topic: str, partition: int, offset: int) -> Message:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "unused", "enable.auto.commit": False}
    )
    try:
        consumer.assign([TopicPartition(topic, partition, offset)])
        deadline = time.monotonic() + READ_TIMEOUT_S
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None and message.error() is None:
                return message
        raise AssertionError(f"nothing at {topic}[{partition}]@{offset} within {READ_TIMEOUT_S} s")
    finally:
        consumer.close()
