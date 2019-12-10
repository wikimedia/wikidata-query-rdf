package org.wikidata.query.rdf.tool.change;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.stream.Collectors.toMap;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.change.events.ChangeEvent;
import org.wikidata.query.rdf.tool.change.events.EventWithChronology;
import org.wikidata.query.rdf.tool.change.events.PageDeleteEvent;
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * This poller will connect to Kafka event source and get changes from
 * one or more topics there.
 *
 * After each block of changes is processed, we store current offsets in the DB as:
 * {@code
 * <http://www.wikidata.org> wikibase:kafka ( "topic:0" 1234 )
 * }
 *
 * When the poller is started, the offsets are initialized from two things:
 * a) if the storage above if found, use that for initialization
 * b) if not, use the given timestamp to initialize
 *
 * This also means if the Kafka offset is ever reset, the old offsets need to be manually
 * cleaned up. See KafkaOffsetsRepository.UPDATE_OFFSETS query for how to do it.
 *
 * See https://wikitech.wikimedia.org/wiki/EventBus for more docs on events in Kafka.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity") // TODO: refactoring required!
public class KafkaPoller implements Change.Source<KafkaPoller.Batch> {

    private static final Logger log = LoggerFactory.getLogger(KafkaPoller.class);

    private static final String MAX_POLL_PROPERTY = KafkaPoller.class.getName() + ".maxPoll";
    private static final String MAX_FETCH_PROPERTY = KafkaPoller.class.getName() + ".maxFetch";
    /**
     * Name of the topic which offset reporting will be based on.
     */
    private static final String REPORTING_TOPIC_PROP = KafkaPoller.class.getName() + ".reportingTopic";

    /**
     * List of topics to listen to.
     * mediawiki.revision-create
     * mediawiki.page-delete
     * mediawiki.page-undelete
     * mediawiki.page-properties-change
     * Note that these may not be real topic names if we have
     * cluster configuration.
     * FIXME: should be configuration?
     */
    private static final Map<String, Class<? extends ChangeEvent>> defaultTopics = ImmutableMap.of(
            // Not not using for now since revision-create should cover it
//                  "mediawiki.recentchange", RecentChangeEvent.class,
            "mediawiki.revision-create", RevisionCreateEvent.class,
            "mediawiki.page-delete", PageDeleteEvent.class,
            // Same class as revision-create since relevant data part looks the same
            "mediawiki.page-undelete", RevisionCreateEvent.class
//            "mediawiki.page-properties-change", PropertiesChangeEvent.class
    );

    /**
     * Default name of the topic which offset reporting will be based on.
     */
    private static final String DEFAULT_REPORTING_TOPIC =  "mediawiki.revision-create";

    /**
     *
     */
    private final String reportingTopic;

    /**
     * The first start time to poll.
     */
    private final Instant firstStartTime;
    /**
     * Size of the batches to poll against wikibase.
     */
    private final int batchSize;
    /**
     * Kafka consumer.
     */
    private final Consumer<String, ChangeEvent> consumer;
    /**
     * Used to store and retrieve Kafka Offsets.
     */
    private final KafkaOffsetsRepository kafkaOffsetsRepository;
    /**
     * Wikibase URIs setup.
     */
    private Uris uris;
    /**
     * List of topics/partitions we're listening to.
     */
    @Nonnull
    private final ImmutableList<TopicPartition> topicPartitions;
    /**
     * Should we ignore stored offsets?
     */
    private final boolean ignoreStoredOffsets;

    /**
     * Counts the total number of changes polled from Kafka.
     */
    private final Counter changesCounter;

    /**
     * Counts the time spent polling Kafka.
     *
     * Note that this includes time spent waiting for a timeout if not enough
     * changes are available in Kafka for a poll to complete immediately.
     */
    private final Timer pollingTimer;

    private final Queue<Map<TopicPartition, OffsetAndMetadata>> offsetsToCommit = new ConcurrentLinkedQueue<>();

    public KafkaPoller(Consumer<String, ChangeEvent> consumer, Uris uris,
                       Instant firstStartTime, int batchSize, Collection<String> topics,
                       KafkaOffsetsRepository kafkaOffsetsRepository,
                       boolean ignoreStoredOffsets,
                       MetricRegistry metricRegistry) {
        this.consumer = consumer;
        this.uris = uris;
        this.firstStartTime = firstStartTime;
        this.batchSize = batchSize;
        this.changesCounter = metricRegistry.counter("kafka-changes-counter");
        this.pollingTimer = metricRegistry.timer("kafka-changes-timer");
        this.topicPartitions = topicsToPartitions(topics, consumer);
        this.kafkaOffsetsRepository = kafkaOffsetsRepository;
        this.ignoreStoredOffsets = ignoreStoredOffsets;
        this.reportingTopic = System.getProperty(REPORTING_TOPIC_PROP, DEFAULT_REPORTING_TOPIC);
    }

    /**
     * Create list of topics with cluster names.
     * @param clusterNames Cluster names (if empty, original list is returned)
     * @return List of topics with cluster names as: %cluster%.%topic%
     */
    private static Map<String, Class<? extends ChangeEvent>> clusterNamesAwareTopics(Collection<String> clusterNames) {
        if (clusterNames == null || clusterNames.isEmpty()) {
            // No cluster - use topic names as is
            return defaultTopics;
        } else {
            return defaultTopics.entrySet().stream().flatMap(entry ->
                // Prepend cluster names to keys, e.g.:
                // page.revision => eqiad.page.revision
                clusterNames.stream().map(
                    cluster -> Maps.immutableEntry(cluster + "." + entry.getKey(), entry.getValue())
                )
            ).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    // Suppressing resource warnings so Java doesn't complain about KafkaConsumer not being closed
    @SuppressWarnings("resource")
    private static KafkaConsumer<String, ChangeEvent> buildKafkaConsumer(
            String servers, String consumerId,
            Map<String, Class<? extends ChangeEvent>> topicToClass,
            int batchSize) {
        // See http://kafka.apache.org/documentation.html#consumerconfigs
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", consumerId);
        // This is an interval between polls after which the broker decides the client is dead.
        // 5 mins seems to be good enough.
        props.put("max.poll.interval.ms", "600000");
        // We will manually commit after the batch is processed
        props.put("enable.auto.commit", "false");
        // See https://cwiki.apache.org/confluence/display/KAFKA/KIP-41%3A+KafkaConsumer+Max+Records
        // Basically it works this way: Kafka fetches N records from each partition, where N is max.poll.records
        // Or if there isn't as much, it polls as many as possible.
        // Then it returns them to poll() in a round-robin fashion. Next poll is not initiated unless
        // the prefetch data dips below N.
        // TODO: Should we set it to half batch size, so in each batch we will have several topics?
        props.put("max.poll.records", System.getProperty(MAX_POLL_PROPERTY, String.valueOf(batchSize)));
        // This is about one batch of messages since message sizes in event queue are about 1k
        props.put("max.partition.fetch.bytes", System.getProperty(MAX_FETCH_PROPERTY, String.valueOf(batchSize * 1024)));
        log.info("Creating consumer {}", consumerId);
        return new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(topicToClass));
    }

    @Nonnull
    public static KafkaPoller buildKafkaPoller(
            String kafkaServers, String consumerId, Collection<String> clusterNames,
            Uris uris, int batchSize, Instant startTime,
            boolean ignoreStoredOffsets, KafkaOffsetsRepository kafkaOffsetsRepository,
            MetricRegistry metricRegistry) {
        if (consumerId == null) {
            throw new IllegalArgumentException("Consumer ID (--consumer) must be set");
        }
        Map<String, Class<? extends ChangeEvent>> topicsToClass = clusterNamesAwareTopics(clusterNames);
        ImmutableSet<String> topics = ImmutableSet.copyOf(topicsToClass.keySet());

        return new KafkaPoller(
                buildKafkaConsumer(kafkaServers, consumerId, topicsToClass, batchSize),
                uris, startTime, batchSize, topics,
                kafkaOffsetsRepository,
                ignoreStoredOffsets,
                metricRegistry);
    }

    @Override
    public Batch firstBatch() throws RetryableException {
        Map<TopicPartition, OffsetAndTimestamp> kafkaOffsets = fetchOffsets();
        // assign ourselves to all partitions of the topics we want
        consumer.assign(kafkaOffsets.keySet());
        log.info("Subscribed to {} topics", kafkaOffsets.size());
        // Seek each topic to proper offset
        kafkaOffsets.forEach(
                (topic, offset) -> {
                    if (offset == null) {
                        log.info("No offset for {}, starting at the end", topic);
                        consumer.seekToEnd(Collections.singletonList(topic));
                        return;
                    }
                    consumer.seek(topic, offset.offset());
                    log.info("Set topic {} to {}", topic, offset);
                }
         );

        return fetch(firstStartTime);
    }

    /**
     * Fetch current offsets for all topics.
     * The offsets come either from persistent offset storage or
     * from offsetsForTimes() API.
     * @return Map TopicPartition -> offset
     */
    private Map<TopicPartition, OffsetAndTimestamp> fetchOffsets() {
        // Create a map of offsets from storage
        Map<TopicPartition, OffsetAndTimestamp> storedOffsets;
        if (ignoreStoredOffsets) {
            storedOffsets = ImmutableMap.of();
        } else {
            storedOffsets = kafkaOffsetsRepository.load(firstStartTime);
        }

        // Make a map (topic, partition) -> timestamp for those not in loaded map
        Map<TopicPartition, Long> topicParts = topicPartitions.stream()
                .filter(tp -> !storedOffsets.containsKey(tp))
                .collect(toMap(o -> o, o -> firstStartTime.toEpochMilli()));

        // Remove topics that are not supported anymore
        Map<TopicPartition, OffsetAndTimestamp> results = storedOffsets
                .entrySet().stream()
                .filter(e -> topicPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue));

        // Fill up missing offsets from timestamp
        if (topicParts.size() > 0) {
            results.putAll(consumer.offsetsForTimes(topicParts));
        }

        return results;
    }

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        return fetch(lastBatch.leftOffDate());
    }

    @Override
    public void done(Batch batch) {
        offsetsToCommit.offer(batch.offsets);
        // We store offsets to the repository synchronously
        // TODO: get rid of the offsets here, there should be no need
        // to store offsets in different stores
        kafkaOffsetsRepository.store(batch.offsets);
    }

    /**
     * Fetch changes from Kafka.
     * @param lastNextStartTime where last fetch ended up.
     * @return Set of changes.
     * @throws RetryableException
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    private Batch fetch(Instant lastNextStartTime) throws RetryableException {
        Map<String, Change> changesByTitle = new LinkedHashMap<>();
        ConsumerRecords<String, ChangeEvent> records;
        Instant nextInstant = Instant.EPOCH;
        AtomicLongMap<String> topicCounts = AtomicLongMap.create();
        Map<TopicPartition, OffsetAndMetadata> batchOffsets = new HashMap<>();
        while (true) {
            commitPendindOffsets();
            try (Context timerContext = pollingTimer.time()) {
                // TODO: make timeout configurable? Wait for a bit so we catch bursts of messages?
                records = consumer.poll(1000);
            } catch (InterruptException | WakeupException e) {
                throw new RetryableException("Error fetching recent changes", e);
            }
            int count = records.count();
            log.debug("Fetched {} records from Kafka", count);
            changesCounter.inc(count);
            if (count == 0) {
                // If we got nothing from Kafka, get out of the loop and return what we have
                break;
            }
            boolean foundSomething = false;
            for (ConsumerRecord<String, ChangeEvent> record: records) {
                ChangeEvent event = record.value();
                String topic = record.topic();
                batchOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                log.trace("Got event t:{} o:{}", record.topic(), record.offset());
                if (!event.domain().equals(uris.getHost())) {
                    // wrong domain, ignore
                    continue;
                }
                // Not checking timestamp here, since different channels can have different
                // idea about what the latest timestamp is.
                // check namespace
                if (!uris.isEntityNamespace(event.namespace())) {
                    continue;
                }
                // Now we have event that we want to process
                foundSomething = true;
                topicCounts.getAndIncrement(record.topic());
                // Keep max time for the reporting topic
                // We use only one topic (or set of topics) here because otherwise when catching up we
                // could get messages from different topics with different times and the tracking becomes
                // very chaotic, jumping back and forth.
                if (topic.endsWith(reportingTopic)) {
                    nextInstant = Utils.max(nextInstant, Instant.ofEpochMilli(record.timestamp()));
                }
                // Using offset here as RC id since we do not have real RC id (this not being RC poller) but
                // the offset serves the same function in Kafka and is also useful for debugging.
                Change change = makeChange(event, record.offset());
                Change dupe = changesByTitle.put(change.entityId(), change);
                // If we have a duplicate - i.e. event with same title - we
                // keep the newest one, by revision number, or in case of deletion, we keep
                // the delete since delete don't have it's own revision number.
                // Note that rev numbers are used only to weed out repeated changes
                // so worst thing we do extra work. This can happen if delete is duplicated
                // or combined with other event, since deletes will always be included.
                // This is not a big deal since deletes are relatively rare.
                if (dupe != null && change.revision() > Change.NO_REVISION && (dupe.revision() > change.revision() || dupe.revision() == Change.NO_REVISION)) {
                    // need to remove so that order will be correct
                    changesByTitle.remove(change.entityId());
                    changesByTitle.put(change.entityId(), dupe);
                }

            }
            log.debug("{} records left after filtering", changesByTitle.size());
            if (changesByTitle.size() >= batchSize) {
                // We have enough for the batch
                break;
            }
            if (changesByTitle.size() > 0 && !foundSomething) {
                log.info("Did not find anything useful in this batch, returning existing data");
                // We have changes and last poll didn't find anything new - return these ones, don't
                // wait for more.
                break;
            }
            // TODO: if we already have something and we've spent more than X seconds in the loop,
            // we probably should return without waiting for more
        }

        // If we didn't get anything useful in the reporting topic, keep the old value
        if (nextInstant.equals(Instant.EPOCH)) {
            nextInstant = lastNextStartTime;
        }

        final ImmutableList<Change> changes = ImmutableList.copyOf(changesByTitle.values());
        log.info("Found {} changes", changes.size());
        if (log.isDebugEnabled()) {
            topicCounts.asMap().forEach((k, v) -> log.debug("Topic {}: {} records", k, v));
        }
        long advanced = ChronoUnit.MILLIS.between(lastNextStartTime, nextInstant);
        // Show the user the polled time - one second because we can't
        // be sure we got the whole second
        return new Batch(changes, advanced, nextInstant.minusSeconds(1).toString(), nextInstant, batchOffsets);
    }

    /**
     * Create change object from event.
     */
    private Change makeChange(ChangeEvent event, long position) {
        if (event instanceof EventWithChronology) {
            return new Change(event.title(), event.revision(), event.timestamp(), position, ((EventWithChronology) event).chronologyId());
        } else {
            return new Change(event.title(), event.revision(), event.timestamp(), position);
        }
    }

    /**
     * Set up the list of partitions for topics we're interested in.
     */
    private static ImmutableList<TopicPartition> topicsToPartitions(
            Collection<String> topics,
            Consumer<String, ChangeEvent> consumer) {
        return topics.stream()
                // Get partitions
                .flatMap((String topic) -> consumer.partitionsFor(topic).stream())
                // Create TopicPartition objects
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(toImmutableList());
    }

    public static final class Batch extends Change.Batch.AbstractDefaultImplementation {
        /**
         * The date where we last left off.
         */
        private final Instant leftOffDate;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public Batch(ImmutableList<Change> changes, long advanced, String leftOff, Instant nextStartTime, Map<TopicPartition, OffsetAndMetadata> offsets) {
            super(changes, advanced, leftOff);
            leftOffDate = nextStartTime;
            this.offsets = offsets;
        }

        @Override
        public String advancedUnits() {
            return "milliseconds";
        }

        @Override
        public Instant leftOffDate() {
            return leftOffDate;
        }
    }

    private void commitPendindOffsets() {
        // Commit asynchronously pending offsets, they should be managed by the next poll
        // We assume that failing couple commits is OK as long as it is temporary
        // c.f. https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html#idm45788273408552
        // section Combining Synchronous and Asynchronous Commits
        // Here the synchronous commits are handled by the close method
        // Continuous failures to commit offsets will be seen by monitoring classic kafka consumer lag metrics
        Map<TopicPartition, OffsetAndMetadata> offsets;
        while ((offsets = offsetsToCommit.poll()) != null) {
            consumer.commitAsync(offsets, (o, e) -> {
                if (e != null) {
                    log.warn("Failed to commit offsets to kafka", e);
                }
            });
        }
    }

    @Override
    public void close() {
        Map<TopicPartition, OffsetAndMetadata> lastOffsets = null;
        while (true) {
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsToCommit.poll();
            if (offsets == null) {
                break;
            }
            lastOffsets = offsets;
        }
        if (lastOffsets != null) {
            consumer.commitSync(lastOffsets);
        }
        consumer.close();
    }

}
