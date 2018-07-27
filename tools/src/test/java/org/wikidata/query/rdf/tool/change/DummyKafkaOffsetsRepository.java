package org.wikidata.query.rdf.tool.change;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.ImmutableMap;

/**
 * Dummy offsets repository store, used for testing.
 *
 * This repository never stores anything and always returns en empty offsets map.
 */
public class DummyKafkaOffsetsRepository implements KafkaOffsetsRepository {

    /** Load an empty offsets map. */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> load(Instant firstStartTime) {
        return ImmutableMap.of();
    }

    /** Ignores the partitionsAndOffsets parameter and stores nothing. */
    @Override
    public void store(Map<TopicPartition, Long> partitionsAndOffsets) {

    }
}
