#!/usr/bin/env python3
import os
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.environ.get("KAFKA_INPUT_TOPIC", "input-topic")
OUTPUT_TOPIC = os.environ.get("KAFKA_OUTPUT_TOPIC", "output-topic")
PARTITIONS_INPUT = int(os.environ.get("KAFKA_INPUT_PARTITIONS", "1"))
PARTITIONS_OUTPUT = int(os.environ.get("KAFKA_OUTPUT_PARTITIONS", "1"))
REPLICATION_FACTOR = int(os.environ.get("KAFKA_REPLICATION_FACTOR", "1"))


def log(msg: str):
    print(f"[create_topics.py] {msg}")

def ensure_topics(admin: KafkaAdminClient, specs):
    existing = set(admin.list_topics())
    to_create = []
    for name, partitions in specs:
        if name not in existing:
            to_create.append(NewTopic(name=name, num_partitions=partitions, replication_factor=REPLICATION_FACTOR, topic_configs={'message.timestamp.type': 'LogAppendTime'}))
        else:
            config_resource = ConfigResource(
                resource_type=ConfigResourceType.TOPIC,
                name=name,
                configs={'message.timestamp.type': 'LogAppendTime'})
            admin.alter_configs([config_resource])
    if to_create:
        try:
            admin.create_topics(to_create, validate_only=False)
            log(f"Created topics: {[t.name for t in to_create]}")
        except TopicAlreadyExistsError:
            log("Topics already existed (race)")


def main():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="devcontainer-setup")
    try:
        ensure_topics(
            admin,
            [
                (INPUT_TOPIC, PARTITIONS_INPUT),
                (OUTPUT_TOPIC, PARTITIONS_OUTPUT),
            ],
        )
    finally:
        admin.close()
    log("Done.")


if __name__ == "__main__":
    main()
