package org.wikidata.query.rdf.tool.change;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.change.events.ChangeEvent;
import org.wikidata.query.rdf.tool.change.events.PageDeleteEvent;
import org.wikidata.query.rdf.tool.change.events.PropertiesChangeEvent;
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * This poller will connect to Kafka event source and get changes from
 * one or more topics there.
 * See https://wikitech.wikimedia.org/wiki/EventBus for more docs on events in Kafka.
 */
public class KafkaPoller implements Change.Source<KafkaPoller.Batch> {
    private static final Logger log = LoggerFactory.getLogger(KafkaPoller.class);

    private static final String MAX_POLL_PROPERTY = KafkaPoller.class.getName() + ".maxPoll";
    private static final String MAX_FETCH_PROPERTY = KafkaPoller.class.getName() + ".maxFetch";
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
            "mediawiki.page-undelete", RevisionCreateEvent.class,
            "mediawiki.page-properties-change", PropertiesChangeEvent.class
    );
    /**
     * List of actual topic names, with cluster names prepended.
     */
    private final Collection<String> topics;
    /**
     * Wikibase URIs setup.
     */
    private Uris uris;

    @Override
    public Batch firstBatch() throws RetryableException {
        // consumer.subscribe(topicList);
        Map<TopicPartition, Long> topicParts = newHashMapWithExpectedSize(topics.size());
        // Make a map (topic, partition) -> timestamp
        for (String topic: topics) {
            for (PartitionInfo partition: consumer.partitionsFor(topic)) {
                topicParts.put(new TopicPartition(topic, partition.partition()), firstStartTime.toEpochMilli());
            }
        }
//        Map<TopicPartition, Long> topicParts = topics.stream().flatMap(topic ->
//            consumer.partitionsFor(topic).stream().map(partition ->
//                Maps.immutableEntry(new TopicPartition(topic, partition.partition()), firstStartTime.getTime())
//            )
//        ).collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

        // assign ourselves to all partitions of the topics we want
        consumer.assign(topicParts.keySet());
        log.info("Subscribed to {} topics", topicParts.size());
        // Seek each topic to proper offset
        consumer.offsetsForTimes(topicParts).forEach(
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

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        consumer.commitSync();
        return fetch(lastBatch.leftOffDate());
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
        Map<String, Instant> timesByTopic = newHashMapWithExpectedSize(topics.size());
        while (true) {
            try {
                // TODO: make timeout configurable? Wait for a bit so we catch bursts of messages?
                records = consumer.poll(1000);
            } catch (InterruptException | WakeupException e) {
                throw new RetryableException("Error fetching recent changes", e);
            }
            int count = records.count();
            log.info("Fetched {} records from Kafka", count);
            if (count == 0) {
                // If we got nothing from Kafka, get out of the loop and return what we have
                break;
            }
            boolean foundSomething = false;
            for (ConsumerRecord<String, ChangeEvent> record: records) {
                ChangeEvent event = record.value();
                String topic = record.topic();
                log.debug("Got event t:{} o:{}", record.topic(), record.offset());
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
                if (event.isRedundant()) {
                    // This is a redundant event, we can skip it.
                    continue;
                }
                // Now we have event that we want to process
                foundSomething = true;
                // Keep max time per topic
                timesByTopic.put(topic, Utils.max(Instant.ofEpochMilli(record.timestamp()),
                                timesByTopic.getOrDefault(topic, null)));
                // Using offset here as RC id since we do not have real RC id (this not being RC poller) but
                // the offset serves the same function in Kafka and is also useful for debugging.
                Change change = new Change(event.title(), event.revision(), event.timestamp(), record.offset());
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
        // Here we are using min, not max, since some topics may be lagging behind
        // and we should catch them up if we are restarted.
        // Note that this means we could get repeated items from more advanced topics,
        // but that's ok, since changes are checked by revid anyway.
        // This is important only on updater restart, otherwise Kafka offsets should
        // take care of tracking things.
        Instant nextInstant = timesByTopic.values().stream().min(Instant::compareTo)
                .orElse(lastNextStartTime);
        // FIXME: Note that due to batching nature of Kafka this timestamp could actually jump
        // back and forth. Not sure what to do about it.

        final ImmutableList<Change> changes = ImmutableList.copyOf(changesByTitle.values());
        log.info("Found {} changes", changes.size());
        long advanced = ChronoUnit.MILLIS.between(lastNextStartTime, nextInstant);
        // Show the user the polled time - one second because we can't
        // be sure we got the whole second
        return new Batch(changes, advanced, nextInstant.minusSeconds(1).toString(), nextInstant);
    }

    public KafkaPoller(Consumer<String, ChangeEvent> consumer, Uris uris,
            Instant firstStartTime, int batchSize, Collection<String> topics) {
        this.consumer = consumer;
        this.uris = uris;
        this.firstStartTime = firstStartTime;
        this.batchSize = batchSize;
        this.topics = topics;
    }

    public static final class Batch extends Change.Batch.AbstractDefaultImplementation {
        /**
         * The date where we last left off.
         */
        private final Instant leftOffDate;

        public Batch(ImmutableList<Change> changes, long advanced, String leftOff, Instant nextStartTime) {
            super(changes, advanced, leftOff);
            leftOffDate = nextStartTime;
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

    public static Map<String, Class<? extends ChangeEvent>> clusterNamesAwareTopics(Collection<String> clusterNames) {
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
            ).collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
        }
    }

    // Suppressing resource warnings so Java doesn't complain about KafkaConsumer not being closed
    @SuppressWarnings("resource")
    public static KafkaConsumer<String, ChangeEvent> buildKafkaConsumer(String servers, Map<String, Class<? extends ChangeEvent>> topicToClass, int batchSize) {
        // See http://kafka.apache.org/documentation.html#consumerconfigs
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        // Random group ID because we don't want to be grouped with anybody else.
        props.put("group.id", UUID.randomUUID().toString());
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
        // We set it to half batch size, so in each batch we will have several topics.
        props.put("max.poll.records", System.getProperty(MAX_POLL_PROPERTY, String.valueOf(batchSize)));
        // This is about one batch of messages since message sizes in event queue are about 1k
        props.put("max.partition.fetch.bytes", System.getProperty(MAX_FETCH_PROPERTY, String.valueOf(batchSize * 1024)));

        return new KafkaConsumer<>(props, new StringDeserializer(), new JsonDeserializer<>(topicToClass));
    }

    @Nonnull
    public static KafkaPoller buildKafkaPoller(
            String kafkaServers, Collection<String> clusterNames,
            Uris uris, int batchSize, Instant startTime) {
        Map<String, Class<? extends ChangeEvent>> topicsToClass = clusterNamesAwareTopics(clusterNames);
        ImmutableSet<String> topics = ImmutableSet.copyOf(topicsToClass.keySet());

        return new KafkaPoller(buildKafkaConsumer(kafkaServers, topicsToClass, batchSize),
                uris, startTime, batchSize, topics);
    }

    @Override
    public void close() {
        consumer.close();
    }

}
