#!/usr/bin/python3.7

# Simple tool to initialize offsets for the flink streaming updater
# The timestamp is obtained by running:
# val dump_date = "20200601"
# spark.read.parquet("hdfs://analytics-hadoop/wmf/data/discovery/wdqs/ttl/" + dump_date)
#   .select("object")
#   .filter("subject = '<http://wikiba.se/ontology#Dump>' and predicate = '<http://schema.org/dateModified>'")
#   .orderBy(asc("object"))
#   .show(1, false)
#
# using spark2-shell --master yarn
#
# Example call:
# python3.7 set_offsets.py \
#   -t eqiad.mediawiki.revision-create \
#   -c test_wdqs_streaming_updater \
#   -b kafka-jumbo1001.eqiad.wmnet:9092 \
#   -s 2020-05-30T20:26:47

from argparse import ArgumentParser
from datetime import datetime
from typing import Dict, List

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndTimestamp


def get_offsets(consumer: KafkaConsumer, topics: List[str], timestamp: datetime) -> Dict[TopicPartition, OffsetAndTimestamp]:
    topic_partitions = []
    for topic in topics:
        for p in consumer.partitions_for_topic(topic):
            topic_partitions.append(TopicPartition(topic, p))
    return consumer.offsets_for_times({tp: timestamp.timestamp()*1000 for tp in topic_partitions})


def set_offsets(consumer: KafkaConsumer, offsets: Dict[TopicPartition, OffsetAndTimestamp]) -> None:
    consumer.assign(list(offsets.keys()))
    for topic, offset in offsets.items():
        print("Setting: " + str(topic) + " to " + str(offset))
        consumer.seek(topic, offset.offset)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        '-t', '--topics', nargs="+",
        help='Topics to configure')
    parser.add_argument(
        '-c', '--consumer',
        help='Consumer group to configure')
    parser.add_argument(
        '-b', '--brokers',
        help='Comma separated list of brokers to connect to')
    parser.add_argument(
        '-s', '--timestamp', type=lambda s: datetime.fromisoformat(s),
        help="Timestamp to seek offsets for")
    args = parser.parse_args()
    print("Configuring offsets for {cons}@{brokers} for topics {topics} at time {time}".format(
        cons=args.consumer,
        brokers=args.brokers,
        topics=args.topics,
        time=args.timestamp
    ))
    kconsumer = KafkaConsumer(
        bootstrap_servers=args.brokers,
        group_id=args.consumer
    )
    set_offsets(consumer=kconsumer, offsets=get_offsets(consumer=kconsumer, topics=args.topics, timestamp=args.timestamp))
    kconsumer.commit()
    kconsumer.close(autocommit=False)
