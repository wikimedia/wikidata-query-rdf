package org.wikidata.query.rdf.updater.consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.change.JsonDeserializer;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;

@NotThreadSafe
public class KafkaStreamConsumer implements StreamConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamConsumer.class);
    private final Consumer<String, MutationEventData> consumer;
    private final TopicPartition topicPartition;
    private final LinkedHashSet<ConsumerRecord<String, MutationEventData>> buffer;
    private final RDFChunkDeserializer rdfDeser;
    private final Predicate<MutationEventData> messageFilter;
    private final int bufferedInputMessages;
    private final int preferredBatchLength;
    private final KafkaStreamConsumerMetricsListener metrics;

    private Map<TopicPartition, OffsetAndMetadata> lastPendingOffsets;
    private Map<TopicPartition, OffsetAndMetadata> lastOfferedBatchOffsets;
    private Instant lastBatchEventTime;

    public static BiConsumer<Consumer<String, MutationEventData>, TopicPartition> resetToTime(Instant time) {
        return (consumer, topic) -> {
            Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = consumer.offsetsForTimes(singletonMap(topic, time.getEpochSecond()));
            OffsetAndTimestamp offsetAndTimestamp = offsetAndTimestampMap.get(topic);
            if (offsetAndTimestamp == null) {
                throw new IllegalStateException("Cannot reset kafka offsets to " + time.toString() + ". This position has been found in the stream.");
            }
            BiConsumer<Consumer<String, MutationEventData>, TopicPartition> offsetReset = resetToOffset(offsetAndTimestamp.offset());
            offsetReset.accept(consumer, topic);
        };
    }

    public static BiConsumer<Consumer<String, MutationEventData>, TopicPartition> resetToOffset(long offset) {
        return (consumer, topicPartition) -> consumer.seek(topicPartition, offset);
    }

    public static KafkaStreamConsumer build(String brokers, String topic, int partition, String consumerId, int maxBatchLength, RDFChunkDeserializer deser,
                                            @Nullable BiConsumer<Consumer<String, MutationEventData>, TopicPartition> offsetReset,
                                            KafkaStreamConsumerMetricsListener metrics, int bufferedInputMessages, Predicate<MutationEventData> filter) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", consumerId);
        props.put("max.poll.interval.ms", "600000");
        props.put("enable.auto.commit", "false");
        props.put("isolation.level", "read_committed");
        props.put("max.poll.records", bufferedInputMessages);
        if (offsetReset == null) {
            props.put("auto.offset.reset", "earliest");
        } else {
            props.put("auto.offset.reset", "none");
        }
        props.put("max.partition.fetch.bytes", 10*120*1024); // 10 very large messages (120k)
        KafkaConsumer<String, MutationEventData> consumer = new KafkaConsumer<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(singletonMap(topic, MutationEventData.class)));
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(singleton(new TopicPartition(topic, partition)));
        try {
            // Fetching position will fail if no offsets are positioned yet for this consumerId.
            // This pattern only works because we know that we have a single consumer per blazegraph host.
            // If it was a group of consumers like it's usually the case this strategy would make no sense.
            consumer.position(topicPartition);
        } catch (InvalidOffsetException ioe) {
            if (offsetReset == null) {
                throw new IllegalStateException("Failed to find earliest offsets for [" + topicPartition + "]", ioe);
            }
            offsetReset.accept(consumer, topicPartition);
        }
        return new KafkaStreamConsumer(consumer, topicPartition, deser, maxBatchLength, metrics, bufferedInputMessages, filter);
    }

    public KafkaStreamConsumer(KafkaConsumer<String, MutationEventData> consumer, TopicPartition topicPartition,
                               RDFChunkDeserializer rdfDeser, int preferredBatchLength, KafkaStreamConsumerMetricsListener metrics,
                               int bufferedInputMessages, Predicate<MutationEventData> filter) {
        this.consumer = consumer;
        this.topicPartition = topicPartition;
        this.rdfDeser = rdfDeser;
        if (preferredBatchLength <= 0) {
            throw new IllegalArgumentException("preferredBatchLength must be strictly positive: " + preferredBatchLength + " given");
        }
        this.preferredBatchLength = preferredBatchLength;
        this.metrics = metrics;
        this.bufferedInputMessages = bufferedInputMessages;
        buffer = new LinkedHashSet<>(bufferedInputMessages);
        messageFilter = filter;
    }

    public Batch poll(Duration timeout) {
        if (lastOfferedBatchOffsets != null) {
            throw new IllegalStateException("Last batch must be acknowledged before polling a new one.");
        }
        PatchAccumulator accumulator = new PatchAccumulator(rdfDeser);
        ConsumerRecord<String, MutationEventData> lastRecord = null;
        long st = System.nanoTime();
        // latency on the average of the event dates is similar to the average of the latencies
        long sumEvenTimes = 0;
        long nbEvents = 0;
        while (!timeout.isNegative() && accumulator.weight() < preferredBatchLength) {
            if (buffer.size() < bufferedInputMessages) {
                ConsumerRecords<String, MutationEventData> records = consumer.poll(timeout);
                timeout = timeout.minusNanos(System.nanoTime() - st);
                records.forEach(this::filterAndBufferMessage);
            }
            // Accept partial messages if our buffer is already full
            List<ConsumerRecord<String, MutationEventData>> entityChunks = reassembleMessage(buffer, buffer.size() >= bufferedInputMessages);
            if (entityChunks.isEmpty()) {
                continue;
            }
            if (entityChunks.stream().map(ConsumerRecord::value).anyMatch(m -> !accumulator.canAccumulate(m))) {
                break;
            }

            nbEvents++;
            lastRecord = entityChunks.get(entityChunks.size() - 1);
            sumEvenTimes += lastRecord.value().getEventTime().toEpochMilli();
            accumulator.accumulate(entityChunks.stream().map(ConsumerRecord::value).collect(toList()));
            buffer.removeAll(entityChunks);
        }
        metrics.triplesAccum(accumulator.getTotalAccumulated());
        metrics.triplesOffered(accumulator.getNumberOfTriples());
        metrics.deletedEntities(accumulator.getNumberOfDeletedEntities());
        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata;
        if (lastRecord != null) {
            offsetsAndMetadata = singletonMap(topicPartition, new OffsetAndMetadata(lastRecord.offset()));
            lastBatchEventTime = Instant.ofEpochMilli(sumEvenTimes / nbEvents);
            lastOfferedBatchOffsets = offsetsAndMetadata;
            return new Batch(accumulator.asPatch(), lastBatchEventTime);
        } else {
            return null;
        }
    }

    /**
     * Buffers the message if it passes the mutationFilter predicate.
     */
    private void filterAndBufferMessage(ConsumerRecord<String, MutationEventData> message) {
        if (messageFilter.test(message.value())) {
            buffer.add(message);
        }
    }

    private List<ConsumerRecord<String, MutationEventData>> reassembleMessage(Iterable<ConsumerRecord<String, MutationEventData>> records,
                                                                              boolean acceptPartial) {
        List<ConsumerRecord<String, MutationEventData>> entityChunks = new ArrayList<>();
        for (ConsumerRecord<String, MutationEventData> record : records) {
            entityChunks.add(record);
            if (record.value().getSequence() + 1 == record.value().getSequenceLength()) {
                return entityChunks;
            }
        }
        return acceptPartial ? entityChunks : emptyList();
    }

    @Override
    public void acknowledge() {
        if (lastOfferedBatchOffsets != null) {
            metrics.lag(lastBatchEventTime);
            lastBatchEventTime = null;
            lastPendingOffsets = lastOfferedBatchOffsets;
            consumer.commitAsync(lastPendingOffsets, this::offsetCommitCallback);
            lastOfferedBatchOffsets = null;
        }
    }

    @Override
    public void close() {
        if (lastPendingOffsets != null) {
            consumer.commitSync(lastPendingOffsets);
            lastPendingOffsets = null;
        }
        consumer.close();
    }

    private void offsetCommitCallback(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, Exception e) {
        if (e == null && offsetsToCommit.equals(lastPendingOffsets)) {
            // we succeeded
            lastPendingOffsets = null;
        } else if (e != null && offsetsToCommit.equals(lastPendingOffsets)) {
            // safe to retry if these are the latest offsets we intended to commit
            LOG.warn("Failed to commit offsets (retrying)", e);
            consumer.commitAsync(offsetsToCommit, this::offsetCommitCallback);
        } else if (e != null) {
            // we just skip those otherwise as the ones
            LOG.warn("Failed to commit offsets (skipping)", e);
        }
    }
}
