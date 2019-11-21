package org.wikidata.query.rdf.tool.change;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

/**
 * Store and retrieve Kafka offsets.
 */
public interface KafkaOffsetsRepository {

    /**
     * Load offsets from storage.
     *
     * If no offset is found in storage, use firstStartTime as a starting point.
     */
    Map<TopicPartition, OffsetAndTimestamp> load(Instant firstStartTime);

    /**
     * Store Kafka offsets.
     */
    void store(Map<TopicPartition, OffsetAndMetadata> partitionsAndOffsets);
}
