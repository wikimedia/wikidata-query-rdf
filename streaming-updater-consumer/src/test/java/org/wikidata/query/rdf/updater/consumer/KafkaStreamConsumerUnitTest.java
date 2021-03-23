package org.wikidata.query.rdf.updater.consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.updater.DiffEventData;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataGenerator;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;
import org.wikidata.query.rdf.updater.RDFChunkSerializer;
import org.wikidata.query.rdf.updater.RDFDataChunk;

import com.codahale.metrics.MetricRegistry;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamConsumerUnitTest {
    private static final String TEST_DOMAIN = "tested.unittest.local";
    private static final String TESTED_STREAM = "tested_stream";
    private static final int TESTED_PARTITION = 0;
    public static final int BUFFERED_INPUT_MESSAGES = 250;
    @Mock
    private KafkaConsumer<String, MutationEventData> consumer;
    private final RDFChunkDeserializer chunkDeser = new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance()));
    private final RDFChunkSerializer chunkSer = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
    private final MutationEventDataGenerator generator = new MutationEventDataGenerator(chunkSer, RDFFormat.TURTLE.getDefaultMIMEType(), 100);
    private final Set<Statement> storedData = new HashSet<>(statements(uris("Q1-removed-0", "Q1-shared")));
    private final Set<Statement> expectedData = new HashSet<>(statements(uris(
            "Q1-added-0",
            "Q1-shared",
            "Q1-added-2",
            "Q1-added-3",
            "Q1-added-4",
            "Q1-added-1",
            "Q1-shared-2"
    )));
    private final Set<Statement> unlinkedSharedElts = new HashSet<>();
    private final Set<Statement> expectedUnlinkedElts = new HashSet<>(statements(uris(
            "Q1-shared-3"
    )));
    private final Set<Statement> expectedUnlinkedEltsSingleOptimizedBatch = new HashSet<>(statements(uris(
            "Q1-shared-3",
            "Q1-shared"
    )));
    private final Set<String> deletedEntityIds = new HashSet<>();
    private static final String REQ_ID = UUID.randomUUID().toString();
    private static final Supplier<EventsMeta> EVENTS_META_SUPPLIER =
            () -> new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, REQ_ID);


    @Test
    public void test_poll_accumulates_records_into_a_rdfpatch() {
        TopicPartition topicPartition = new TopicPartition("test", 0);

        // preferredBatchLength set to 4 end when reading event n°4 and not rely on timeouts to batch 5 and 6
        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 8,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), 250, m -> true);
        List<ConsumerRecord<String, MutationEventData>> allRecords = recordsList(Instant.EPOCH, Duration.ofMinutes(2));
        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, 2))),
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(2, allRecords.size()))),
                new ConsumerRecords<>(emptyMap()));

        List<Instant> eventTimes = new ArrayList<>();

        while (true) {
            StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(1000));
            if (b == null) {
                break;
            }
            ConsumerPatch patch = b.getPatch();
            eventTimes.add(b.getAverageEventTime());
            storedData.addAll(patch.getAdded());
            storedData.removeAll(patch.getRemoved());
            storedData.addAll(patch.getLinkedSharedElements());
            unlinkedSharedElts.addAll(patch.getUnlinkedSharedElements());
            deletedEntityIds.addAll(patch.getEntityIdsToDelete());
            streamConsumer.acknowledge();
        }

        streamConsumer.close();
        assertThat(eventTimes).containsExactly(
            Instant.EPOCH.plus(Duration.ofMinutes((2 + 2 * 2 + 2 * 3 + 2 * 4) / 4)),
            Instant.EPOCH.plus(Duration.ofMinutes((2 * 5 + 2 * 6) / 2))
        );
        assertThat(storedData).containsExactlyInAnyOrderElementsOf(expectedData);
        assertThat(unlinkedSharedElts.size()).isBetween(1, 2);
        if (unlinkedSharedElts.size() == 1) {
            assertThat(unlinkedSharedElts).containsExactlyInAnyOrderElementsOf(expectedUnlinkedElts);
        } else {
            assertThat(unlinkedSharedElts).containsExactlyInAnyOrderElementsOf(expectedUnlinkedEltsSingleOptimizedBatch);
        }
        assertThat(deletedEntityIds).containsOnly("Q2");
    }

    @Test
    public void test_poll_accumulates_records_with_delete_into_a_rdfpatch() {
        TopicPartition topicPartition = new TopicPartition("test", 0);

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, Integer.MAX_VALUE,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), 250, m -> true);
        List<ConsumerRecord<String, MutationEventData>> allRecords = recordListWithDeleteAndAdd();
        when(consumer.poll(any())).thenReturn(
            new ConsumerRecords<>(singletonMap(topicPartition, allRecords)),
            new ConsumerRecords<>(emptyMap()));

        Set<Statement> actualStoredData = new HashSet<>();
        Set<String> actualDeletedEntities = new HashSet<>();
        Set<Statement> expectedAddedData = new HashSet<>(statements(uris(
                "Q1-added-0",
                "Q1-shared"
        )));
        while (true) {
            StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(100));
            if (b == null) {
                break;
            }
            ConsumerPatch patch = b.getPatch();
            actualStoredData.addAll(patch.getAdded());
            actualStoredData.removeAll(patch.getRemoved());
            actualStoredData.addAll(patch.getLinkedSharedElements());
            actualDeletedEntities.addAll(patch.getEntityIdsToDelete());
            streamConsumer.acknowledge();
        }
        streamConsumer.close();
        assertThat(actualStoredData).containsExactlyInAnyOrderElementsOf(expectedAddedData);
        assertThat(actualDeletedEntities).containsOnly("Q1");
    }

    @Test
    public void test_prefer_reassembled_message() {
        TopicPartition topicPartition = new TopicPartition("test", 0);
        List<ConsumerRecord<String, MutationEventData>> allRecords = IntStream.range(0, BUFFERED_INPUT_MESSAGES).mapToObj(i -> {
            EventsMeta meta = new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, "unused");
            MutationEventData diff = new DiffEventData(meta, "Q1", 1, Instant.EPOCH, i, BUFFERED_INPUT_MESSAGES, MutationEventData.DIFF_OPERATION,
                    new RDFDataChunk("<uri:a> <uri:a> <uri:" + i + "> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                    null, null, null);
            return new ConsumerRecord<String, MutationEventData>(topicPartition.topic(), topicPartition.partition(), i, null, diff);
        }).collect(toList());
        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, BUFFERED_INPUT_MESSAGES / 2))),
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(BUFFERED_INPUT_MESSAGES / 2, allRecords.size()))),
                new ConsumerRecords<>(emptyMap()));

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), 250, m -> true);
        StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNotNull();
        ConsumerPatch patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(BUFFERED_INPUT_MESSAGES);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNull();
    }

    @Test
    public void test_allow_large_partial_message() {
        TopicPartition topicPartition = new TopicPartition("test", 0);
        int l = BUFFERED_INPUT_MESSAGES + 1;
        List<ConsumerRecord<String, MutationEventData>> allRecords = IntStream.range(0, l).mapToObj(i -> {
           EventsMeta meta = new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, "unused");
           MutationEventData diff = new DiffEventData(meta, "Q1", 1, Instant.EPOCH, i, l, MutationEventData.DIFF_OPERATION,
                   new RDFDataChunk("<uri:a> <uri:a> <uri:" + i + "> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                   null, null, null);
           return new ConsumerRecord<String, MutationEventData>(topicPartition.topic(), topicPartition.partition(), i, null, diff);
        }).collect(toList());
        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, BUFFERED_INPUT_MESSAGES))),
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(BUFFERED_INPUT_MESSAGES, allRecords.size()))),
                new ConsumerRecords<>(emptyMap()));

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), BUFFERED_INPUT_MESSAGES, m -> true);
        StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNotNull();
        ConsumerPatch patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(BUFFERED_INPUT_MESSAGES);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNotNull();
        patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(1);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNull();
    }

    @Test
    public void test_commit_offsets() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        MutationEventData firstEvent = genEvent("Q1", 0, uris("uri:1"), uris(), uris(), uris(), Instant.EPOCH).get(0);
        MutationEventData secondEvent = genEvent("Q1", 1, uris("uri:2"), uris(), uris(), uris(), Instant.EPOCH).get(0);
        MutationEventData thirdEvent = genEvent("Q1", 2, uris("uri:3"), uris(), uris(), uris(), Instant.EPOCH).get(0);
        Map<TopicPartition, OffsetAndMetadata> firstOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(1));
        Map<TopicPartition, OffsetAndMetadata> secondOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(2));
        Map<TopicPartition, OffsetAndMetadata> thirdOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(3));
        // we want real instances as we use AtomicReference

        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 1, null, firstEvent)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 2, null, secondEvent)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 3, null, thirdEvent)))),
                new ConsumerRecords<>(emptyMap()));

        ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 1,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), 250, m -> true);
        StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(10));
        streamConsumer.acknowledge();
        verify(consumer, times(1)).commitAsync(eq(firstOffsets), callback.capture());

        streamConsumer.poll(Duration.ofMillis(10));

        // fail the first commit and verify that we retry
        callback.getValue().onComplete(firstOffsets, new Exception("simulated failure"));
        verify(consumer, times(2)).commitAsync(eq(firstOffsets), callback.capture());

        streamConsumer.acknowledge();

        // fail the first commit a second time after we are ready to commit the second batch
        // and verify that we do not retry
        callback.getValue().onComplete(firstOffsets, new Exception("simulated failure"));
        verify(consumer, times(2)).commitAsync(eq(firstOffsets), callback.capture());

        // also verify that we send commitAsync for the second batch
        verify(consumer, times(1)).commitAsync(eq(secondOffsets), callback.capture());

        // fail the second commit and verify that we retry
        callback.getValue().onComplete(secondOffsets, new Exception("Simulated failure"));
        verify(consumer, times(2)).commitAsync(eq(secondOffsets), callback.capture());
        // the retry succeeded
        callback.getValue().onComplete(secondOffsets, null);

        streamConsumer.poll(Duration.ofMillis(10));
        streamConsumer.acknowledge();
        verify(consumer, times(1)).commitAsync(eq(thirdOffsets), callback.capture());
        streamConsumer.close();
        // verify that we commit synchronously since we did not receive yet the ack of our async commit
        verify(consumer, times(1)).commitSync(eq(thirdOffsets));
    }

    @Test
    public void test_messages_can_be_filtered() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        MutationEventData event1 = genEvent("Q1", 1, uris("Q1-added-0"), uris(), uris("Q1-shared"), uris(), Instant.EPOCH).get(0);
        MutationEventData event2 = genEvent("L1", 1, uris("L1-added-0"), uris(), uris("L1-shared"), uris(), Instant.EPOCH).get(0);

        when(consumer.poll(any())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 1, null, event1)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 2, null, event2)))),
            new ConsumerRecords<>(emptyMap()));


        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()), BUFFERED_INPUT_MESSAGES, m -> m.getEntity().matches("^L.*"));
        StreamConsumer.Batch b = streamConsumer.poll(Duration.ofMillis(100));
        assertThat(b).isNotNull();
        assertThat(b.getPatch().getRemoved()).isEmpty();
        assertThat(b.getPatch().getUnlinkedSharedElements()).isEmpty();
        assertThat(b.getPatch().getAdded()).containsExactlyElementsOf(statements(uris("L1-added-0")));
        assertThat(b.getPatch().getLinkedSharedElements()).containsExactlyElementsOf(statements(uris("L1-shared")));
    }

    private List<ConsumerRecord<String, MutationEventData>> recordsList(Instant  eventTimeBase, Duration eventTimeIncrease) {
        List<MutationEventData> events = Stream.of(
                genEvent("Q1", 1, uris("Q1-added-0"), uris(), uris("Q1-shared"), uris(),
                        eventTimeBase.plus(eventTimeIncrease.multipliedBy(1))),
                genEvent("Q1", 2, uris("Q1-added-1", "Q1-added-2"), uris("Q1-removed-0"), uris(), uris(),
                        eventTimeBase.plus(eventTimeIncrease.multipliedBy(2))),
                genEvent("Q1", 3, uris("Q1-added-3", "Q1-added-4", "Q1-added-5"), uris("Q1-added-1"), uris(), uris(),
                        eventTimeBase.plus(eventTimeIncrease.multipliedBy(3))),
                genEvent("Q1", 4, uris("Q1-added-1"), uris("Q1-added-5"), uris("Q1-shared-2"), uris("Q1-shared", "Q1-shared-3"),
                        eventTimeBase.plus(eventTimeIncrease.multipliedBy(4))),
                genEvent("Q2", 1, uris("Q2-added-0"), uris(), uris("Q1-shared"), uris(), eventTimeBase.plus(eventTimeIncrease.multipliedBy(5))),
                genDeleteEvent("Q2", 2, eventTimeBase.plus(eventTimeIncrease.multipliedBy(6)))
        ).flatMap(Collection::stream).collect(toList());

        return IntStream.range(0, events.size())
                .mapToObj(i -> new ConsumerRecord<String, MutationEventData>(TESTED_STREAM, TESTED_PARTITION, i, null, events.get(i)))
                .collect(toList());
    }

    private List<ConsumerRecord<String, MutationEventData>> recordListWithDeleteAndAdd() {
        List<MutationEventData> events = Stream.of(
                genEvent("Q1", 1, uris("Q1-added-0"), uris(), uris("Q1-shared"), uris(), Instant.EPOCH),
                genDeleteEvent("Q1", 1, Instant.EPOCH),
                genEvent("Q1", 1, uris("Q1-added-0"), uris(), uris("Q1-shared"), uris(), Instant.EPOCH)
        ).flatMap(Collection::stream).collect(toList());

        return IntStream.range(0, events.size())
                .mapToObj(i -> new ConsumerRecord<String, MutationEventData>(TESTED_STREAM, TESTED_PARTITION, i, null, events.get(i)))
                .collect(toList());
    }

    private String[] uris(String...uris) {
        return Arrays.stream(uris).map(u -> "uri:" + u).toArray(String[]::new);
    }

    private List<? extends MutationEventData> genEvent(String entity, int rev, String[] added, String[] deleted,
                                                       String[] linkedSharedElt, String[] unlinkedSharedElts, Instant eventTime) {
        if (deleted.length == 0) {
            return generator.fullImportEvent(EVENTS_META_SUPPLIER, entity, rev, eventTime, statements(added), statements(linkedSharedElt));
        } else {
            return generator.diffEvent(EVENTS_META_SUPPLIER, entity, rev, eventTime, statements(added), statements(deleted),
                    statements(linkedSharedElt), statements(unlinkedSharedElts));
        }
    }

    private List<? extends MutationEventData> genDeleteEvent(String entity, int rev, Instant eventTime) {
        return generator.deleteEvent(EVENTS_META_SUPPLIER, entity, rev, eventTime);
    }
}
