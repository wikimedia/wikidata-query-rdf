package org.wikidata.query.rdf.tool.stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import java.time.Instant;
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
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.tool.rdf.RDFPatch;

import com.codahale.metrics.MetricRegistry;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamConsumerUnitTest {
    private static final String TEST_DOMAIN = "tested.unittest.local";
    private static final String TESTED_STREAM = "tested_stream";
    private static final int TESTED_PARTITION = 0;
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

    private List<ConsumerRecord<String, MutationEventData>> recordsList() {
        List<MutationEventData> events = Stream.of(
                genEvent("Q1", 1, uris("Q1-added-0"), uris(), uris("Q1-shared"), uris()),
                genEvent("Q1", 2, uris("Q1-added-1", "Q1-added-2"), uris("Q1-removed-0"), uris(), uris()),
                genEvent("Q1", 3, uris("Q1-added-3", "Q1-added-4", "Q1-added-5"), uris("Q1-added-1"), uris(), uris()),
                genEvent("Q1", 4, uris("Q1-added-1"), uris("Q1-added-5"), uris("Q1-shared-2"), uris("Q1-shared", "Q1-shared-3"))
        ).flatMap(Collection::stream).collect(toList());

        return IntStream.range(0, events.size())
                .mapToObj(i -> new ConsumerRecord<String, MutationEventData>(TESTED_STREAM, TESTED_PARTITION, i, null, events.get(i)))
                .collect(toList());
    }

    @Test
    public void test_poll_accumulates_records_into_a_rdfpatch() {
        TopicPartition topicPartition = new TopicPartition("test", 0);

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()));
        List<ConsumerRecord<String, MutationEventData>> allRecords = recordsList();
        when(consumer.poll(anyLong())).thenReturn(
            new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, 2))),
            new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(2, allRecords.size()))),
            new ConsumerRecords<>(emptyMap()));

        while (true) {
            StreamConsumer.Batch b = streamConsumer.poll(100);
            if (b == null) {
                break;
            }
            RDFPatch patch = b.getPatch();
            storedData.addAll(patch.getAdded());
            storedData.removeAll(patch.getRemoved());
            storedData.addAll(patch.getLinkedSharedElements());
            unlinkedSharedElts.addAll(patch.getUnlinkedSharedElements());
            streamConsumer.acknowledge();
        }
        streamConsumer.close();
        assertThat(storedData).containsExactlyInAnyOrderElementsOf(expectedData);
        assertThat(unlinkedSharedElts.size()).isBetween(1, 2);
        if (unlinkedSharedElts.size() == 1) {
            assertThat(unlinkedSharedElts).containsExactlyInAnyOrderElementsOf(expectedUnlinkedElts);
        } else {
            assertThat(unlinkedSharedElts).containsExactlyInAnyOrderElementsOf(expectedUnlinkedEltsSingleOptimizedBatch);
        }
    }

    @Test
    public void test_prefer_reassembled_message() {
        TopicPartition topicPartition = new TopicPartition("test", 0);
        List<ConsumerRecord<String, MutationEventData>> allRecords = IntStream.range(0, KafkaStreamConsumer.SOFT_BUFFER_CAP).mapToObj(i -> {
            EventsMeta meta = new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, "unused");
            MutationEventData diff = new DiffEventData(meta, "Q1", 1, Instant.EPOCH, i, KafkaStreamConsumer.SOFT_BUFFER_CAP, MutationEventData.DIFF_OPERATION,
                    new RDFDataChunk("<uri:a> <uri:a> <uri:" + i + "> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                    null, null, null);
            return new ConsumerRecord<String, MutationEventData>(topicPartition.topic(), topicPartition.partition(), i, null, diff);
        }).collect(toList());
        when(consumer.poll(anyLong())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, KafkaStreamConsumer.SOFT_BUFFER_CAP / 2))),
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(KafkaStreamConsumer.SOFT_BUFFER_CAP / 2, allRecords.size()))),
                new ConsumerRecords<>(emptyMap()));

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()));
        StreamConsumer.Batch b = streamConsumer.poll(100);
        assertThat(b).isNotNull();
        RDFPatch patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(KafkaStreamConsumer.SOFT_BUFFER_CAP);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(100);
        assertThat(b).isNull();
    }

    @Test
    public void test_allow_large_partial_message() {
        TopicPartition topicPartition = new TopicPartition("test", 0);
        int l = KafkaStreamConsumer.SOFT_BUFFER_CAP + 1;
        List<ConsumerRecord<String, MutationEventData>> allRecords = IntStream.range(0, l).mapToObj(i -> {
           EventsMeta meta = new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, "unused");
           MutationEventData diff = new DiffEventData(meta, "Q1", 1, Instant.EPOCH, i, l, MutationEventData.DIFF_OPERATION,
                   new RDFDataChunk("<uri:a> <uri:a> <uri:" + i + "> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                   null, null, null);
           return new ConsumerRecord<String, MutationEventData>(topicPartition.topic(), topicPartition.partition(), i, null, diff);
        }).collect(toList());
        when(consumer.poll(anyLong())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(0, KafkaStreamConsumer.SOFT_BUFFER_CAP))),
                new ConsumerRecords<>(singletonMap(topicPartition, allRecords.subList(KafkaStreamConsumer.SOFT_BUFFER_CAP, allRecords.size()))),
                new ConsumerRecords<>(emptyMap()));

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 10,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()));
        StreamConsumer.Batch b = streamConsumer.poll(100);
        assertThat(b).isNotNull();
        RDFPatch patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(KafkaStreamConsumer.SOFT_BUFFER_CAP);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(100);
        assertThat(b).isNotNull();
        patch = b.getPatch();
        assertThat(patch.getAdded().size()).isEqualTo(1);
        streamConsumer.acknowledge();
        b = streamConsumer.poll(100);
        assertThat(b).isNull();
    }

    @Test
    public void test_commit_offsets() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        MutationEventData firstEvent = genEvent("Q1", 0, uris("uri:1"), uris(), uris(), uris()).get(0);
        MutationEventData secondEvent = genEvent("Q1", 1, uris("uri:2"), uris(), uris(), uris()).get(0);
        MutationEventData thirdEvent = genEvent("Q1", 2, uris("uri:3"), uris(), uris(), uris()).get(0);
        Map<TopicPartition, OffsetAndMetadata> firstOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(1));
        Map<TopicPartition, OffsetAndMetadata> secondOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(2));
        Map<TopicPartition, OffsetAndMetadata> thirdOffsets = Collections.singletonMap(topicPartition, new OffsetAndMetadata(3));
        // we want real instances as we use AtomicReference

        when(consumer.poll(anyLong())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 1, null, firstEvent)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 2, null, secondEvent)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(TESTED_STREAM, 0, 3, null, thirdEvent)))),
                new ConsumerRecords<>(emptyMap()));

        ArgumentCaptor<OffsetCommitCallback> callback = ArgumentCaptor.forClass(OffsetCommitCallback.class);

        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 1,
                KafkaStreamConsumerMetricsListener.forRegistry(new MetricRegistry()));
        StreamConsumer.Batch b = streamConsumer.poll(10);
        streamConsumer.acknowledge();
        verify(consumer, times(1)).commitAsync(eq(firstOffsets), callback.capture());

        streamConsumer.poll(10);

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

        streamConsumer.poll(10);
        streamConsumer.acknowledge();
        verify(consumer, times(1)).commitAsync(eq(thirdOffsets), callback.capture());
        streamConsumer.close();
        // verify that we commit synchronously since we did not receive yet the ack of our async commit
        verify(consumer, times(1)).commitSync(eq(thirdOffsets));
    }

    private String[] uris(String...uris) {
        return Arrays.stream(uris).map(u -> "uri:" + u).toArray(String[]::new);
    }

    private List<? extends MutationEventData> genEvent(String entity, int rev, String[] added, String[] deleted,
                                                       String[] linkedSharedElt, String[] unlinkedSharedElts) {
        String reqId = UUID.randomUUID().toString();
        Supplier<EventsMeta> supplier = () -> new EventsMeta(Instant.EPOCH, UUID.randomUUID().toString(), TEST_DOMAIN, TESTED_STREAM, reqId);
        if (deleted.length == 0) {
            return generator.fullImportEvent(supplier, entity, rev, Instant.EPOCH, statements(added), statements(linkedSharedElt));
        } else {
            return generator.diffEvent(supplier, entity, rev, Instant.EPOCH, statements(added), statements(deleted),
                    statements(linkedSharedElt), statements(unlinkedSharedElts));
        }
    }
}
