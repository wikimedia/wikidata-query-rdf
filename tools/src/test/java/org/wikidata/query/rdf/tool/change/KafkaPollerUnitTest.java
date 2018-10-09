package org.wikidata.query.rdf.tool.change;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.tool.change.events.ChangeEventFixtures.DOMAIN;
import static org.wikidata.query.rdf.tool.change.events.ChangeEventFixtures.START_TIME;
import static org.wikidata.query.rdf.tool.change.events.ChangeEventFixtures.makeDeleteEvent;
import static org.wikidata.query.rdf.tool.change.events.ChangeEventFixtures.makeRCEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.tool.change.KafkaPoller.Batch;
import org.wikidata.query.rdf.tool.change.events.ChangeEvent;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class KafkaPollerUnitTest {

    @Mock
    private KafkaConsumer<String, ChangeEvent> consumer;
    private Uris uris;

    private static final ConsumerRecords<String, ChangeEvent> EMPTY_CHANGES =
            new ConsumerRecords<>(Collections.emptyMap());
    private static final int BATCH_SIZE = 5;

    @Before
    public void setupUris() {
        uris = Uris.fromString("https://" + DOMAIN);
        uris.setEntityNamespaces(new long[] {0});
    }

    @Test
    public void noChanges() throws RetryableException {
        KafkaPoller poller = makePoller();
        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes()).hasSize(0);
        assertThat(batch.hasAnyChanges()).isFalse();
    }

    @Test
    public void changesFromTopics() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 1, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(21), 1, "Q234"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(20), 1, "Q567"), "topictest", Duration.ofMillis(20))
        );
        Batch batch = getBatchFromRecords(rs);

        assertThat(batch.changes())
                .hasSize(3)
                .anyMatch(title("Q123"))
                .anyMatch(title("Q234"))
                .anyMatch(title("Q567"));
    }

    @Test
    public void changesOrder() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q123"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q123"), "othertopic", Duration.ofMillis(15)),
                makeRecord(makeRCEvent(Duration.ofMillis(35), 7, "Q123"), "topictest", Duration.ofMillis(25))
        );
        Batch batch = getBatchFromRecords(rs);

        // There should be only one change, and it should have max revision
        assertThat(batch.changes())
                .hasSize(1)
                .anyMatch(revision(10));
    }

    @Test
    public void filterOtherChanges() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );
        Batch batch = getBatchFromRecords(rs);

        // There should be only one change, and it should have max revision
        assertThat(batch.changes())
                .hasSize(1)
                .anyMatch(title("Q123"));
    }

    @Test
    public void multiPolls() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q234"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 10, "Q123"), "othertopic", Duration.ofMillis(31)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 21, "Q245"), "topictest", Duration.ofMillis(40))
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();
        // second batch did not have good messages, so the poller should return
        // before third batch
        assertThat(batch.changes())
                .hasSize(1)
                .anyMatch(titleRevision("Q123", 5));

        batch = poller.nextBatch(batch);
        assertThat(batch.changes())
                .hasSize(3)
                .anyMatch(titleRevision("Q123", 10))
                .anyMatch(title("Q234"))
                .anyMatch(title("Q245"));
    }

    @Test
    public void multiPolls2() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 1, "Q234"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q234"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 10, "Q123"), "othertopic", Duration.ofMillis(31)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 21, "Q245"), "topictest", Duration.ofMillis(40))
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();

        // If all three had good events, all three should be in the batch
        assertThat(batch.changes())
                .hasSize(3)
                .anyMatch(titleRevision("Q123", 10))
                .anyMatch(titleRevision("Q234", 2))
                .anyMatch(titleRevision("Q245", 21));
    }

    @Test
    public void batchSize() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q1"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q666", 1, DOMAIN), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q2"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q3"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(20), 5, "Q4"), "topictest", Duration.ofMillis(20))
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 10, "Q3"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 20, "Q1"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 100, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 20, "Q2"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 20, "Q1"), "othertopic", Duration.ofMillis(21))
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 100, "Q3"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 200, "Q1"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 100, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 200, "Q5"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 200, "Q6"), "othertopic", Duration.ofMillis(21))
        );
        ConsumerRecords<String, ChangeEvent> rs4 = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q7"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(25), 10, "Q6666", 0, "acme.wrong"), "topictest", Duration.ofMillis(20))
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, rs4, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();

        // The batch should stop as soon as we got over size 5
        assertThat(batch.changes())
                .hasSize(6)
                .anyMatch(titleRevision("Q1", 200))
                .anyMatch(titleRevision("Q2", 20))
                .anyMatch(titleRevision("Q3", 100))
                .anyMatch(titleRevision("Q4", 5))
                .anyMatch(titleRevision("Q5", 200))
                .anyMatch(titleRevision("Q6", 200))
                .noneMatch(title("Q7"));
    }

    @Test
    public void deleteRevision() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 1, "Q123"), "topictest", Duration.ofMillis(20)),
                makeRecord(makeDeleteEvent(Duration.ofMillis(21), "Q123"), "othertopic", Duration.ofMillis(21)),
                makeRecord(makeRCEvent(Duration.ofMillis(22), 2, "Q123"), "topictest", Duration.ofMillis(22))
        );
        Batch batch = getBatchFromRecords(rs);
        // Delete revision should always win
        assertThat(batch.changes())
                .hasSize(1)
                .anyMatch(revision(-1));
    }

    @Test
    public void advanceTimestamp() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(Duration.ofMillis(20), 1, "Q123"), "mediawiki.revision-create", Duration.ofMillis(120000)),
                makeRecord(makeRCEvent(Duration.ofMillis(30), 2, "Q234"), "mediawiki.revision-create", Duration.ofMillis(122000)),
                makeRecord(makeDeleteEvent(Duration.ofMillis(21), "Q123"), "othertopic", Duration.ofMillis(121000)),
                makeRecord(makeDeleteEvent(Duration.ofMillis(22), "Q234"), "othertopic", Duration.ofMillis(122000)),
                makeRecord(makeDeleteEvent(Duration.ofMillis(31), "Q123"), "othertopic", Duration.ofMillis(123000))
        );
        Batch batch = getBatchFromRecords(rs);
        // Advancement is minimum over maximal times of the topics
        assertThat(batch.advanced()).isEqualTo(122000L);
        assertThat(batch.leftOffDate()).isEqualTo(START_TIME.plusMillis(122000L));
    }

    @Test
    public void topicSubscribe() throws RetryableException {
        Collection<String> topics = ImmutableList.of("topictest", "othertopic");
        // Each topic gets 2 partitions
        ArgumentCaptor<String> partitionArgs = ArgumentCaptor.forClass(String.class);
        createTopicPartitions(2, partitionArgs);
        // Capture args for assign
        ArgumentCaptor<Collection<TopicPartition>> assignArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).assign(assignArgs.capture());

        when(consumer.offsetsForTimes(any())).thenAnswer(i -> {
            Map<TopicPartition, Long> map = i.getArgumentAt(0, Map.class);
            // Check that timestamps are OK
            map.forEach((k, v) -> assertThat(v).isEqualTo(START_TIME.toEpochMilli()));
            Map<TopicPartition, OffsetAndTimestamp> out = Maps.newHashMapWithExpectedSize(map.size());
            // Make offset 1 for first partition and nothing for second
            map.forEach((k, v) -> out.put(k, k.partition() == 0
                    ? new OffsetAndTimestamp(1000, v)
                    : null));
            // Using forEach here because collect() can't handle nulls
            return out;
        });
        // capture args for seek
        ArgumentCaptor<TopicPartition> seekArgs = ArgumentCaptor.forClass(TopicPartition.class);
        doNothing().when(consumer).seek(seekArgs.capture(), eq(1000L));

        ArgumentCaptor<Collection<TopicPartition>> seekBeginningArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).seekToEnd(seekBeginningArgs.capture());

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        KafkaPoller poller = new KafkaPoller(consumer, uris, START_TIME, BATCH_SIZE, topics, new DummyKafkaOffsetsRepository(), true, new MetricRegistry());
        Batch batch = poller.firstBatch();

        // We get partitions for both topics
        verify(consumer, times(2)).partitionsFor(any());
        assertThat(partitionArgs.getAllValues())
                .contains("topictest", "othertopic");

        // We assign to 4 topics - 2 topics x 2 partitions
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue()).hasSize(4);

        // Calling seek on both topics, partition 0
        verify(consumer, times(2)).seek(any(), anyLong());
        assertThat(seekArgs.getAllValues())
                .extracting(topicPartition -> topicPartition.topic())
                .contains("topictest", "othertopic");

        assertThat(seekArgs.getAllValues())
                .extracting(tp -> tp.partition())
                .hasSize(2)
                .containsOnly(0);

        // Calling seekToEnd on both topics, partition 1
        verify(consumer, times(2)).seekToEnd(any());
        Collection<String> sbTopics = seekBeginningArgs.getAllValues().stream()
                .flatMap(c -> c.stream()).map(tp -> tp.topic())
                .collect(toList());
        assertThat(sbTopics)
                .hasSize(2)
                .contains("topictest", "othertopic");

        Collection<Integer> sbPartitions = seekBeginningArgs.getAllValues().stream()
                .flatMap(c -> c.stream()).map(tp -> tp.partition()).distinct()
                .collect(toList());
        assertThat(sbPartitions)
                .hasSize(1)
                .contains(1);

        verify(consumer, times(1)).offsetsForTimes(any());
    }

    @Test
    public void storedOffsetsFromStorage() throws RetryableException {
        // Scenario where all offsets are loaded from storage
        Collection<String> topics = ImmutableList.of("topictest", "othertopic");
        KafkaOffsetsRepository offsetsRepository = mock(KafkaOffsetsRepository.class);

        createTopicPartitions(2);
        // capture args for assign
        ArgumentCaptor<Collection<TopicPartition>> assignArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).assign(assignArgs.capture());
        // capture args for seek
        ArgumentCaptor<TopicPartition> seekTopics = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> seekOffsets = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumer).seek(seekTopics.capture(), seekOffsets.capture());

        Map<TopicPartition, OffsetAndTimestamp> offsetMap = ImmutableMap.of(
                new TopicPartition("topictest", 0), new OffsetAndTimestamp(1, START_TIME.toEpochMilli()),
                new TopicPartition("topictest", 1), new OffsetAndTimestamp(2, START_TIME.toEpochMilli()),
                new TopicPartition("othertopic", 0), new OffsetAndTimestamp(3, START_TIME.toEpochMilli()),
                new TopicPartition("othertopic", 1), new OffsetAndTimestamp(4, START_TIME.toEpochMilli())
        );

        when(offsetsRepository.load(any())).thenReturn(offsetMap);

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        KafkaPoller poller = new KafkaPoller(
                consumer, uris, START_TIME, BATCH_SIZE, topics,
                offsetsRepository, false, new MetricRegistry());

        Batch batch = poller.firstBatch();
        // should not call offsetsForTimes, since all offsets are in store
        verify(consumer, times(0)).offsetsForTimes(any());
        // We assign to 4 topics - 2 topics x 2 partitions
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue())
                .hasSize(4);
        // Verify topics and offsets
        assertThat(seekTopics.getAllValues())
                .containsExactlyInAnyOrderElementsOf(offsetMap.keySet());

        List<Long> offsets = offsetMap.values().stream()
                .map(o -> o.offset())
                .collect(toList());

        assertThat(seekOffsets.getAllValues())
                .containsExactlyInAnyOrderElementsOf(offsets);
    }

    @Test
    public void storedOffsetsFromBoth() throws RetryableException {
        // Scenario where all offsets are loaded from both storage and timestamp
        Collection<String> topics = ImmutableList.of("topictest", "othertopic", "thirdtopic");
        KafkaOffsetsRepository offsetsRepository = mock(KafkaOffsetsRepository.class);

        createTopicPartitions(1);
        // capture args for assign
        ArgumentCaptor<Collection<TopicPartition>> assignArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).assign(assignArgs.capture());
        // capture args for seek
        ArgumentCaptor<TopicPartition> seekTopics = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> seekOffsets = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumer).seek(seekTopics.capture(), seekOffsets.capture());
        // Stored offsets
        Map<TopicPartition, OffsetAndTimestamp> offsetMap = ImmutableMap.of(
            new TopicPartition("topictest", 0), new OffsetAndTimestamp(1, START_TIME.toEpochMilli()),
            new TopicPartition("othertopic", 0), new OffsetAndTimestamp(3, START_TIME.toEpochMilli())
        );

        when(offsetsRepository.load(any())).thenReturn(offsetMap);

        // Timestamp-driven offsets
        when(consumer.offsetsForTimes(any())).thenAnswer(i -> {
            Map<TopicPartition, Long> map = i.getArgumentAt(0, Map.class);
            // Check that timestamps are OK
            map.forEach((k, v) -> assertThat(v).isEqualTo(START_TIME.toEpochMilli()));
            // All offsets are 500
            return map.entrySet().stream().collect(Collectors.toMap(
                    Entry::getKey, l -> new OffsetAndTimestamp(500L, l.getValue())));
        });

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        KafkaPoller poller = new KafkaPoller(
                consumer, uris, START_TIME, BATCH_SIZE, topics,
                offsetsRepository, false, new MetricRegistry());

        Batch batch = poller.firstBatch();
        // should not call offsetsForTimes, since all offsets are in store
        verify(consumer, times(1)).offsetsForTimes(any());
        // We assign to 3 topics
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue()).hasSize(topics.size());

        assertThat(seekOffsets.getAllValues())
                .hasSize(topics.size())
                .contains(500L); // This offset is from timestamp
    }

    @Test
    public void writeOffsets() throws RetryableException {
        // Scenario where all offsets are loaded from both storage and timestamp
        Collection<String> topics = ImmutableList.of("topictest", "othertopic", "thirdtopic");
        KafkaOffsetsRepository offsetsRepository = mock(KafkaOffsetsRepository.class);

        createTopicPartitions(1);
        when(offsetsRepository.load(any())).thenReturn(ImmutableMap.of());

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        ArgumentCaptor<TopicPartition> positionArgs = ArgumentCaptor.forClass(TopicPartition.class);
        when(consumer.position(positionArgs.capture())).thenReturn(1L, 2L, 3L);

        ArgumentCaptor<Map<TopicPartition, Long>> storeCaptor = ArgumentCaptor.forClass((Class)Map.class);
        doNothing().when(offsetsRepository).store(storeCaptor.capture());

        KafkaPoller poller = new KafkaPoller(
                consumer, uris, START_TIME, BATCH_SIZE, topics,
                offsetsRepository, true, new MetricRegistry());

        Batch batch = poller.firstBatch();
        batch = poller.nextBatch(batch);

        assertThat(positionArgs.getAllValues())
                .containsExactlyInAnyOrder(
                        new TopicPartition("topictest", 0),
                        new TopicPartition("othertopic", 0),
                        new TopicPartition("thirdtopic", 0)
                );

        // Should be one update query
        verify(offsetsRepository, times(1)).store(any());
        assertThat(storeCaptor.getValue())
                .containsEntry(new TopicPartition("topictest", 0), 1L)
                .containsEntry(new TopicPartition("othertopic", 0), 2L)
                .containsEntry(new TopicPartition("thirdtopic", 0), 3L);
    }

    private Predicate<Change> title(String title) {
        return change -> change.entityId().equals(title);
    }

    private Predicate<Change> revision(long revision) {
        return change -> change.revision() == revision;
    }

    private Predicate<Change> titleRevision(String title, int revision) {
        return change -> change.entityId().equals(title) && change.revision() == revision;
    }

    private PartitionInfo makePartitionInfo(String name, int id) {
        return new PartitionInfo(name, id, null, null, null);
    }

    private KafkaPoller makePoller() {
        Collection<String> topics = ImmutableList.of("topictest");

        return new KafkaPoller(consumer, uris, START_TIME, BATCH_SIZE, topics,
                new DummyKafkaOffsetsRepository(), true, new MetricRegistry());
    }

    /**
     * Create record from event.
     */
    private ConsumerRecord<String, ChangeEvent> makeRecord(ChangeEvent event, String topic, Duration offset) {
        return new ConsumerRecord<>(topic, 0, 0, START_TIME.plus(offset).toEpochMilli(),
                TimestampType.LOG_APPEND_TIME, 0L, 0, 0, "", event);
    }

    /**
     * Create ConsumerRecords structure from a set of consumer records.
     */
    private ConsumerRecords<String, ChangeEvent> makeRecords(ConsumerRecord<String, ChangeEvent>... consumerRecords) {
        return new ConsumerRecords<>(
                Arrays.stream(consumerRecords).collect(Collectors.groupingBy(
                        record -> new TopicPartition(record.topic(), record.partition())
                )));
    }

    private Batch getBatchFromRecords(ConsumerRecords<String, ChangeEvent> records) throws RetryableException {
        KafkaPoller poller = makePoller();
        when(consumer.poll(anyLong())).thenReturn(records, EMPTY_CHANGES);

        return poller.firstBatch();
    }

    /**
     * Mock partitionsFor invocation for a number of partitions per topic.
     */
    private void createTopicPartitions(int count, ArgumentCaptor<String> partitionArgs) {
        when(consumer.partitionsFor(partitionArgs.capture())).thenAnswer(inv -> {
            String pName = inv.getArgumentAt(0, String.class);
            List<PartitionInfo> pl = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                pl.add(i, makePartitionInfo(pName, i));
            }
            return pl;
        });
    }

    /**
     * Mock partitionsFor invocation for a number of partitions per topic.
     */
    private void createTopicPartitions(int count) {
        when(consumer.partitionsFor(any())).thenAnswer(inv -> {
            String pName = inv.getArgumentAt(0, String.class);
            List<PartitionInfo> pl = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                pl.add(i, makePartitionInfo(pName, i));
            }
            return pl;
        });
    }

}
