package org.wikidata.query.rdf.tool.change;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class DummyKafkaOffsetsRepositoryUnitTest {

    @Test
    public void loadAlwaysReturnsEmptyMap() {
        DummyKafkaOffsetsRepository offsetsRepository = new DummyKafkaOffsetsRepository();
        Map<TopicPartition, OffsetAndTimestamp> offsets = offsetsRepository.load(Instant.now());

        assertThat(offsets.entrySet()).isEmpty();
    }

    @Test
    public void storeDoesNothing() {
        // This test is mostly useless, but it documents that store is not expected to do anything.
        DummyKafkaOffsetsRepository offsetsRepository = new DummyKafkaOffsetsRepository();

        offsetsRepository.store(ImmutableMap.of(new TopicPartition("partition", 1), 2L));

        Map<TopicPartition, OffsetAndTimestamp> offsets = offsetsRepository.load(Instant.now());
        assertThat(offsets.entrySet()).isEmpty();
    }

}
