package org.wikidata.query.rdf.tool.change;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doNothing;
import static org.wikidata.query.rdf.tool.change.ChangeMatchers.hasTitle;
import static org.wikidata.query.rdf.tool.change.ChangeMatchers.hasRevision;
import static org.wikidata.query.rdf.tool.change.ChangeMatchers.hasTitleRevision;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.mockito.Mock;
import org.wikidata.query.rdf.tool.change.KafkaPoller.Batch;
import org.wikidata.query.rdf.tool.change.events.ChangeEvent;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.tool.change.events.PageDeleteEvent;
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;

@SuppressWarnings("unchecked")
public class KafkaPollerUnitTest {

    @Mock
    private KafkaConsumer<String, ChangeEvent> consumer;
    private Uris uris;

    private static final long BEGIN_DATE = 1518207153000L;
    private static final String DOMAIN = "acme.test";
    private static final ConsumerRecords<String, ChangeEvent> EMPTY_CHANGES =
            new ConsumerRecords<>(Collections.emptyMap());
    private static final int BATCH_SIZE = 5;

    private KafkaPoller makePoller(RdfRepository repo) {
        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);
        Collection<String> topics = ImmutableList.of("topictest");

        return new KafkaPoller(consumer, uris, startTime, BATCH_SIZE, topics, repo);
    }

    private KafkaPoller makePoller() {
        return makePoller(null);
    }

    /**
     * Make valid RC event.
     * @param tsOffset Offset from BEGIN_DATE
     * @param revid Revision ID
     * @param qid Title (Q-id)
     * @return
     */
    private ChangeEvent makeRCEvent(int tsOffset, long revid, String qid) {
        return new RevisionCreateEvent(
                new EventsMeta(Instant.ofEpochMilli(BEGIN_DATE + tsOffset), "", DOMAIN),
                revid, qid, 0);
    }

    private ChangeEvent makeDeleteEvent(int tsOffset, String qid) {
        return new PageDeleteEvent(
                new EventsMeta(Instant.ofEpochMilli(BEGIN_DATE + tsOffset), "", DOMAIN),
                qid, 0);
    }

    /**
     * Make RC event with different namespace and domain.
     * @param tsOffset Offset from BEGIN_DATE
     * @param revid Revision ID
     * @param qid Title (Q-id)
     */
    private ChangeEvent makeRCEvent(int tsOffset, long revid, String qid, int ns, String domain) {
        return new RevisionCreateEvent(
                new EventsMeta(Instant.ofEpochMilli(BEGIN_DATE + tsOffset), "", domain),
                revid, qid, ns);
    }

    /**
     * Create record from event.
     * @param event
     * @param topic
     * @param tsOffset
     * @return
     */
    private ConsumerRecord<String, ChangeEvent> makeRecord(ChangeEvent event, String topic, int tsOffset) {
        return new ConsumerRecord<>(topic, 0, 0, BEGIN_DATE + tsOffset,
                TimestampType.LOG_APPEND_TIME, 0L, 0, 0, "", event);
    }

    /**
     * Create ConsumerRecords structure from a set of consumer records.
     * @param consumerRecords
     * @return
     */
    private ConsumerRecords<String, ChangeEvent> makeRecords(ConsumerRecord<String, ChangeEvent>... consumerRecords) {
        return new ConsumerRecords<String, ChangeEvent>(
                Arrays.stream(consumerRecords).collect(Collectors.groupingBy(
                        record -> new TopicPartition(record.topic(), record.partition())
        )));
    }

    private Batch getBatchFromRecords(ConsumerRecords<String, ChangeEvent> records) throws RetryableException {
        KafkaPoller poller = makePoller();
        when(consumer.poll(anyLong())).thenReturn(records, EMPTY_CHANGES);

        return poller.firstBatch();
    }

    @Test
    public void noChanges() throws RetryableException {
        KafkaPoller poller = makePoller();
        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(0));
        assertFalse(batch.hasAnyChanges());
    }

    @Test
    public void changesFromTopics() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(20, 1, "Q123"), "topictest", 20),
                makeRecord(makeRCEvent(21, 1, "Q234"), "othertopic", 21),
                makeRecord(makeRCEvent(20, 1, "Q567"), "topictest", 20)
        );
        Batch batch = getBatchFromRecords(rs);

        assertThat(batch.changes(), hasSize(3));
        assertThat(batch.changes(), hasItem(hasTitle("Q123")));
        assertThat(batch.changes(), hasItem(hasTitle("Q234")));
        assertThat(batch.changes(), hasItem(hasTitle("Q567")));
    }

    @Test
    public void changesOrder() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(20, 5, "Q123"), "topictest", 20),
                makeRecord(makeRCEvent(30, 2, "Q123"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q123"), "othertopic", 15),
                makeRecord(makeRCEvent(35, 7, "Q123"), "topictest", 25)
        );
        Batch batch = getBatchFromRecords(rs);

        // There should be only one change, and it should have max revision
        assertThat(batch.changes(), hasSize(1));
        assertThat(batch.changes(), hasItem(hasRevision(10)));
    }

    @Test
    public void filterOtherChanges() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(20, 5, "Q123"), "topictest", 20),
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );
        Batch batch = getBatchFromRecords(rs);

        // There should be only one change, and it should have max revision
        assertThat(batch.changes(), hasSize(1));
        assertThat(batch.changes(), hasItem(hasTitle("Q123")));
    }

    @Test
    public void multiPolls() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(20, 5, "Q123"), "topictest", 20),
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(30, 2, "Q234"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20),
                makeRecord(makeRCEvent(30, 10, "Q123"), "othertopic", 31),
                makeRecord(makeRCEvent(30, 21, "Q245"), "topictest", 40)
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();
        // second batch did not have good messages, so the poller should return
        // before third batch
        assertThat(batch.changes(), hasSize(1));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q123", 5)));

        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(3));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q123", 10)));
        assertThat(batch.changes(), hasItem(hasTitle("Q234")));
        assertThat(batch.changes(), hasItem(hasTitle("Q245")));
    }

    @Test
    public void multiPolls2() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(20, 5, "Q123"), "topictest", 20),
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(30, 1, "Q234"), "othertopic", 21),
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(30, 2, "Q234"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20),
                makeRecord(makeRCEvent(30, 10, "Q123"), "othertopic", 31),
                makeRecord(makeRCEvent(30, 21, "Q245"), "topictest", 40)
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();

        // If all three had good events, all three should be in the batch
        assertThat(batch.changes(), hasSize(3));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q123", 10)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q234", 2)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q245", 21)));
    }

    @Test
    public void batchSize() throws RetryableException {
        KafkaPoller poller = makePoller();

        ConsumerRecords<String, ChangeEvent> rs1 = makeRecords(
                makeRecord(makeRCEvent(20, 5, "Q1"), "topictest", 20),
                makeRecord(makeRCEvent(30, 2, "Q666", 1, DOMAIN), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20),
                makeRecord(makeRCEvent(20, 5, "Q2"), "topictest", 20),
                makeRecord(makeRCEvent(20, 5, "Q3"), "topictest", 20),
                makeRecord(makeRCEvent(20, 5, "Q4"), "topictest", 20)
        );
        ConsumerRecords<String, ChangeEvent> rs2 = makeRecords(
                makeRecord(makeRCEvent(30, 10, "Q3"), "othertopic", 21),
                makeRecord(makeRCEvent(30, 20, "Q1"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 100, "Q6666", 0, "acme.wrong"), "topictest", 20),
                makeRecord(makeRCEvent(30, 20, "Q2"), "othertopic", 21),
                makeRecord(makeRCEvent(30, 20, "Q1"), "othertopic", 21)
        );
        ConsumerRecords<String, ChangeEvent> rs3 = makeRecords(
                makeRecord(makeRCEvent(30, 100, "Q3"), "othertopic", 21),
                makeRecord(makeRCEvent(30, 200, "Q1"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 100, "Q6666", 0, "acme.wrong"), "topictest", 20),
                makeRecord(makeRCEvent(30, 200, "Q5"), "othertopic", 21),
                makeRecord(makeRCEvent(30, 200, "Q6"), "othertopic", 21)
        );
        ConsumerRecords<String, ChangeEvent> rs4 = makeRecords(
                makeRecord(makeRCEvent(30, 2, "Q7"), "othertopic", 21),
                makeRecord(makeRCEvent(25, 10, "Q6666", 0, "acme.wrong"), "topictest", 20)
        );

        when(consumer.poll(anyLong())).thenReturn(rs1, rs2, rs3, rs4, EMPTY_CHANGES);
        Batch batch = poller.firstBatch();

        // The batch should stop as soon as we got over size 5
        assertThat(batch.changes(), hasSize(6));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q1", 200)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q2", 20)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q3", 100)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q4", 5)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q5", 200)));
        assertThat(batch.changes(), hasItem(hasTitleRevision("Q6", 200)));
        assertThat(batch.changes(), not(hasItem(hasTitle("Q7"))));
    }

    @Test
    public void deleteRevision() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(20, 1, "Q123"), "topictest", 20),
                makeRecord(makeDeleteEvent(21, "Q123"), "othertopic", 21),
                makeRecord(makeRCEvent(22, 2, "Q123"), "topictest", 22)
        );
        Batch batch = getBatchFromRecords(rs);
        // Delete revision should always win
        assertThat(batch.changes(), hasSize(1));
        assertThat(batch.changes(), hasItem(hasRevision(-1)));
    }

    @Test
    public void advanceTimestamp() throws RetryableException {
        ConsumerRecords<String, ChangeEvent> rs = makeRecords(
                makeRecord(makeRCEvent(20, 1, "Q123"), "topictest", 120000),
                makeRecord(makeRCEvent(30, 2, "Q234"), "topictest", 122000),
                makeRecord(makeDeleteEvent(21, "Q123"), "othertopic", 121000),
                makeRecord(makeDeleteEvent(22, "Q234"), "othertopic", 122000),
                makeRecord(makeDeleteEvent(31, "Q123"), "othertopic", 123000)
        );
        Batch batch = getBatchFromRecords(rs);
        // Advancement is minimum over maximal times of the topics
        assertThat(batch.advanced(), equalTo(122000L));
        assertThat(batch.leftOffDate(), equalTo(Instant.ofEpochMilli(BEGIN_DATE + 122000L)));
    }

    private PartitionInfo makePartitionInfo(String name, int id) {
        return new PartitionInfo(name, id, null, null, null);
    }

    /**
     * Mock partitionsFor invocation for a number of partitions per topic.
     * @param count
     * @param partitionArgs
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
     * @param count
     * @param partitionArgs
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

    @Test
    public void topicSubscribe() throws RetryableException {
        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);
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
            map.forEach((k, v) -> assertThat(v, equalTo(BEGIN_DATE)));
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

        KafkaPoller poller = new KafkaPoller(consumer, uris, startTime, BATCH_SIZE, topics, null);
        Batch batch = poller.firstBatch();

        // We get partitions for both topics
        verify(consumer, times(2)).partitionsFor(any());
        assertThat(partitionArgs.getAllValues(), contains("topictest", "othertopic"));

        // We assign to 4 topics - 2 topics x 2 partitions
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue(), hasSize(4));

        // Calling seek on both topics, partition 0
        verify(consumer, times(2)).seek(any(), anyLong());
        assertThat(seekArgs.getAllValues().stream().map(p -> p.topic()).toArray(), arrayContainingInAnyOrder("topictest", "othertopic"));
        Collection<String> sTopics = seekArgs.getAllValues().stream()
                .map(tp -> tp.topic())
                .collect(Collectors.toList());
        assertThat(sTopics, hasSize(2));
        assertThat(sTopics, containsInAnyOrder("topictest", "othertopic"));
        Collection<Integer> sPartitions = seekArgs.getAllValues().stream()
                .map(tp -> tp.partition()).distinct()
                .collect(Collectors.toList());
        assertThat(sPartitions, hasSize(1));
        assertThat(sPartitions, contains(0));

        // Calling seekToEnd on both topics, partition 1
        verify(consumer, times(2)).seekToEnd(any());
        Collection<String> sbTopics = seekBeginningArgs.getAllValues().stream()
                .flatMap(c -> c.stream()).map(tp -> tp.topic())
                .collect(Collectors.toList());
        assertThat(sbTopics, hasSize(2));
        assertThat(sbTopics, contains("topictest", "othertopic"));
        Collection<Integer> sbPartitions = seekBeginningArgs.getAllValues().stream()
                .flatMap(c -> c.stream()).map(tp -> tp.partition()).distinct()
                .collect(Collectors.toList());
        assertThat(sbPartitions, hasSize(1));
        assertThat(sbPartitions, contains(1));

        verify(consumer, times(1)).offsetsForTimes(any());
    }

    private void repoResults(RdfRepository rdfRepo, ImmutableSetMultimap<String, String> results) {
        when(rdfRepo.selectToMap(any(), eq("topic"), eq("offset"))).thenReturn(results);
    }

    private void repoNoResults(RdfRepository rdfRepo) {
        repoResults(rdfRepo, ImmutableSetMultimap.of());
    }

    @Test
    public void storedOffsetsFromStorage() throws RetryableException {
        // Scenario where all offsets are loaded from storage
        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);
        Collection<String> topics = ImmutableList.of("topictest", "othertopic");
        RdfRepository rdfRepo = mock(RdfRepository.class);

        createTopicPartitions(2);
        // capture args for assign
        ArgumentCaptor<Collection<TopicPartition>> assignArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).assign(assignArgs.capture());
        // capture args for seek
        ArgumentCaptor<TopicPartition> seekTopics = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> seekOffsets = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumer).seek(seekTopics.capture(), seekOffsets.capture());

        ImmutableSetMultimap<String, String> offsetMap = ImmutableSetMultimap.of(
                "topictest:0", "1",
                "topictest:1", "2",
                "othertopic:0", "3",
                "othertopic:1", "4"
        );
        repoResults(rdfRepo, offsetMap);

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        KafkaPoller poller = new KafkaPoller(consumer, uris, startTime, BATCH_SIZE, topics, rdfRepo);

        Batch batch = poller.firstBatch();
        // should not call offsetsForTimes, since all offsets are in store
        verify(consumer, times(0)).offsetsForTimes(any());
        // We assign to 4 topics - 2 topics x 2 partitions
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue(), hasSize(4));
        // Verify topics and offsets
        Set<String> capturedTopics = seekTopics.getAllValues().stream()
                .map(tp -> tp.topic() + ":" + tp.partition())
                .collect(Collectors.toSet());
        assertThat(capturedTopics, containsInAnyOrder(offsetMap.keySet().toArray()));
        Set<String> capturedOffsets = seekOffsets.getAllValues().stream()
                .map(off -> off.toString())
                .collect(Collectors.toSet());
        assertThat(capturedOffsets, containsInAnyOrder(offsetMap.values().toArray()));
    }

    @Test
    public void storedOffsetsFromBoth() throws RetryableException {
        // Scenario where all offsets are loaded from both storage and timestamp
        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);
        Collection<String> topics = ImmutableList.of("topictest", "othertopic", "thirdtopic");
        RdfRepository rdfRepo = mock(RdfRepository.class);

        createTopicPartitions(1);
        // capture args for assign
        ArgumentCaptor<Collection<TopicPartition>> assignArgs = ArgumentCaptor.forClass((Class)Collection.class);
        doNothing().when(consumer).assign(assignArgs.capture());
        // capture args for seek
        ArgumentCaptor<TopicPartition> seekTopics = ArgumentCaptor.forClass(TopicPartition.class);
        ArgumentCaptor<Long> seekOffsets = ArgumentCaptor.forClass(Long.class);
        doNothing().when(consumer).seek(seekTopics.capture(), seekOffsets.capture());
        // Stored offsets
        ImmutableSetMultimap<String, String> offsetMap = ImmutableSetMultimap.of(
                "topictest:0", "1",
                "othertopic:0", "3"
        );
        repoResults(rdfRepo, offsetMap);

        // Timestamp-driven offsets
        when(consumer.offsetsForTimes(any())).thenAnswer(i -> {
            Map<TopicPartition, Long> map = i.getArgumentAt(0, Map.class);
            // Check that timestamps are OK
            map.forEach((k, v) -> assertThat(v, equalTo(BEGIN_DATE)));
            // All offsets are 500
            return map.entrySet().stream().collect(Collectors.toMap(
                    Entry::getKey, l -> new OffsetAndTimestamp(500L, l.getValue())));
        });

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        KafkaPoller poller = new KafkaPoller(consumer, uris, startTime, BATCH_SIZE, topics, rdfRepo);

        Batch batch = poller.firstBatch();
        // should not call offsetsForTimes, since all offsets are in store
        verify(consumer, times(1)).offsetsForTimes(any());
        // We assign to 3 topics
        verify(consumer, times(1)).assign(any());
        assertThat(assignArgs.getValue(), hasSize(topics.size()));

        List<Long> capturedOffsets = seekOffsets.getAllValues();
        assertThat(capturedOffsets, hasSize(topics.size()));
        // This offset is from timestamp
        assertThat(capturedOffsets.get(1), equalTo(Long.valueOf(500L)));
    }

    @Test
    public void writeOffsets() throws RetryableException {
        // Scenario where all offsets are loaded from both storage and timestamp
        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);
        Collection<String> topics = ImmutableList.of("topictest", "othertopic", "thirdtopic");
        RdfRepository rdfRepo = mock(RdfRepository.class);

        createTopicPartitions(1);
        repoNoResults(rdfRepo);

        when(consumer.poll(anyLong())).thenReturn(EMPTY_CHANGES);

        ArgumentCaptor<TopicPartition> positionArgs = ArgumentCaptor.forClass(TopicPartition.class);
        when(consumer.position(positionArgs.capture())).thenReturn(1L, 2L, 3L);

        ArgumentCaptor<String> updateQuery = ArgumentCaptor.forClass(String.class);
        when(rdfRepo.updateQuery(updateQuery.capture())).thenReturn(1);

        KafkaPoller poller = new KafkaPoller(consumer, uris, startTime, BATCH_SIZE, topics, rdfRepo);

        Batch batch = poller.firstBatch();
        batch = poller.nextBatch(batch);

        Set<String> capturedTopics = positionArgs.getAllValues().stream()
                .map(tp -> tp.topic() + ":" + tp.partition())
                .collect(Collectors.toSet());
        assertThat(capturedTopics, containsInAnyOrder("topictest:0",
                "othertopic:0", "thirdtopic:0"));
        // Should be one update query
        verify(rdfRepo, times(1)).updateQuery(any());
        assertThat(updateQuery.getValue(), containsString("wikibase:kafka ( \"topictest:0\" 1 )"));
        assertThat(updateQuery.getValue(), containsString("wikibase:kafka ( \"othertopic:0\" 2 )"));
        assertThat(updateQuery.getValue(), containsString("wikibase:kafka ( \"thirdtopic:0\" 3 )"));
    }

    @Before
    public void setupMocks() {
        MockitoAnnotations.initMocks(this);
        uris = Uris.fromString("https://" + DOMAIN);
        uris.setEntityNamespaces(new long[] {0});
    }

}
