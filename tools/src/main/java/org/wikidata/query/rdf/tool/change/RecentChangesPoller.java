package org.wikidata.query.rdf.tool.change;

import static java.lang.Boolean.TRUE;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.Continue;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.RecentChange;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableList;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Polls recent changes. The first batch polls the recent changes api starting
 * at the firstStartTime. Subsequent batches either start on the last continue
 * of the previous poll, or, if there isn't a continue, then they start one
 * second after the last first start time.
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
@SuppressWarnings("checkstyle:classfanoutcomplexity") // TODO: refactoring required!
public class RecentChangesPoller implements Change.Source<RecentChangesPoller.Batch> {
    private static final Logger LOG = LoggerFactory.getLogger(RecentChangesPoller.class);

    /**
     * How many IDs we keep in seen IDs map.
     * Should be enough for 1 hour for speeds up to 100 updates/s.
     * If we ever get faster updates, we'd have to bump this.
     */
    private static final int MAX_SEEN_IDS = 100 * 60 * 60;
    /**
     * Wikibase repository to poll.
     */
    private final WikibaseRepository wikibase;
    /**
     * The first start time to poll.
     */
    private final Instant firstStartTime;
    /**
     * Size of the batches to poll against wikibase.
     */
    private final int batchSize;
    /**
     * Set of the IDs we've seen before.
     * We have to use map here because LinkedHashMap has removeEldestEntry
     * and it does not implement Set. So we're using
     * Map with boolean values as Set.
     */
    private final Map<Long, Boolean> seenIDs;
    /**
     * How far back should we tail with secondary tailing poller.
     * The value is in milliseconds.
     * 0 or negative means no tailing poller;
     */
    private final int tailSeconds;
    /**
     * How much to back off for recent fetches, in seconds.
     */
    private static final Duration BACKOFF_TIME = Duration.ofSeconds(10);
    /**
     * How old should the change be to not apply backoff.
     */
    private static final Duration BACKOFF_THRESHOLD = Duration.ofMinutes(2);

    /**
     * Queue for communicating with tailing updater.
     */
    private final BlockingQueue<Batch> queue = new ArrayBlockingQueue<>(100);

    /**
     * Optional tailing poller.
     * This will be instantiated only if we were asked for it (tailSeconds > 0)
     * and when the poller catches up enough so that it is beyond the tailing gap
     * itself.
     * Note that tailing poller runs in different thread.
     */
    private TailingChangesPoller tailPoller;

    /**
     * Whether to use backoff.
     */
    private boolean useBackoff = true;

    /**
     * Timer counting the time to taken by each fetch of recent changes.
     */
    private final Timer recentChangesTimer;

    /**
     * Counts the number of changes fetched.
     */
    private final Counter recentChangesCounter;

    public RecentChangesPoller(
            WikibaseRepository wikibase, Instant firstStartTime,
            int batchSize, Map<Long, Boolean> seenIDs, int tailSeconds,
            Timer recentChangesTimer, Counter recentChangesCounter) {
        this.wikibase = wikibase;
        this.firstStartTime = firstStartTime;
        this.batchSize = batchSize;
        this.seenIDs = seenIDs;
        this.tailSeconds = tailSeconds;
        this.recentChangesTimer = recentChangesTimer;
        this.recentChangesCounter = recentChangesCounter;
    }

    public RecentChangesPoller(
            WikibaseRepository wikibase, Instant firstStartTime, int batchSize,
            MetricRegistry metricRegistry) {
        this(wikibase, firstStartTime, batchSize, createSeenMap(), -1,
                metricRegistry.timer("recent-changes-timer"), metricRegistry.counter("recent-changes-counter"));
    }

    public RecentChangesPoller(
            WikibaseRepository wikibase, Instant firstStartTime, int batchSize,
            int tailSeconds, MetricRegistry metricRegistry) {
        this(wikibase, firstStartTime, batchSize, createSeenMap(), tailSeconds,
                metricRegistry.timer("recent-changes-timer"), metricRegistry.counter("recent-changes-counter"));
    }

    /**
     * Whether to use backoff.
     */
    public void setBackoff(boolean useBackoff) {
        this.useBackoff = useBackoff;
    }
    /**
     * Create map of seen IDs.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Map<Long, Boolean> createSeenMap() {
        // Create hash map with max size, evicting oldest ID
        final Map<Long, Boolean> map = new LinkedHashMap(MAX_SEEN_IDS, .75F, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > MAX_SEEN_IDS;
            }
        };
        return Collections.synchronizedMap(map);
    }

    @Override
    public Batch firstBatch() throws RetryableException {
        return batch(firstStartTime, null);
    }

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        Batch newBatch = batch(lastBatch.leftOffDate, lastBatch);
        if (tailSeconds > 0) {
            // Check if tail poller has something to say.
            newBatch = checkTailPoller(newBatch);
        }
        return newBatch;
    }

    /**
     * Check if we need to poll something from secondary poller queue.
     * @param lastBatch
     * @return
     */
    private Batch checkTailPoller(Batch lastBatch) {
        if (tailSeconds <= 0) {
            // not enabled, we're done here
            return lastBatch;
        }

        if (tailPoller == null) {
            if (lastBatch.leftOffDate().isBefore(Instant.now().minusSeconds(tailSeconds))) {
                // still not caught up, do nothing
                return lastBatch;
            }
            // We don't have poller yet - start it
            LOG.info("Started trailing poller with gap of {} seconds", tailSeconds);
            // Create new poller starting back tailSeconds and same IDs map.
            final RecentChangesPoller poller = new RecentChangesPoller(wikibase,
                    Instant.now().minusSeconds(tailSeconds),
                    batchSize, seenIDs, -1, recentChangesTimer, recentChangesCounter);
            poller.setBackoff(false);
            tailPoller = new TailingChangesPoller(poller, queue, tailSeconds);
            tailPoller.setDaemon(true);
            tailPoller.start();
        } else {
            tailPoller.setPollerTs(lastBatch.leftOffDate());
            final Batch queuedBatch = queue.poll();
            if (queuedBatch != null) {
                LOG.info("Merging {} changes from trailing queue", queuedBatch.changes().size());
                return lastBatch.merge(queuedBatch);
            }
        }
        return lastBatch;
    }

    /**
     * Batch implementation for this poller.
     */
    public static final class Batch extends Change.Batch.AbstractDefaultImplementation {
        /**
         * The date where we last left off.
         */
        private final Instant leftOffDate;

        /**
         * Continue from last request. Can be null.
         */
        private final Continue lastContinue;

        /**
         * Flag that states we have had changes, even though we didn't return them.
         */
        private boolean hasChanges;

        /**
         * A batch that will next continue using the continue parameter.
         */
        private Batch(ImmutableList<Change> changes, long advanced,
                String leftOff, Instant nextStartTime, Continue lastContinue) {
            super(changes, advanced, leftOff);
            leftOffDate = nextStartTime;
            this.lastContinue = lastContinue;
        }

        /**
         * Set changes status.
         * @param changes Whether we really had changes or not.
         */
        public void hasChanges(boolean changes) {
            hasChanges = changes;
        }

        @Override
        public boolean hasAnyChanges() {
            if (hasChanges) {
                return true;
            }
            return super.hasAnyChanges();
        }


        @Override
        public String advancedUnits() {
            return "milliseconds";
        }

        @Override
        public Instant leftOffDate() {
            return leftOffDate;
        }

        @Override
        public String leftOffHuman() {
            if (lastContinue != null) {
                return leftOffDate + " (next: " + lastContinue.getRcContinue() + ")";
            } else {
                return leftOffDate.toString();
            }
        }

        /**
         * Merge this batch with another batch.
         */
        @SuppressFBWarnings(value = "OCP_OVERLY_CONCRETE_PARAMETER", justification = "Type seems semantically correct")
        public Batch merge(Batch another) {
            final ImmutableList<Change> newChanges = new ImmutableList.Builder<Change>()
                    .addAll(another.changes())
                    .addAll(changes())
                    .build();
            return new Batch(newChanges, advanced(), leftOffDate.toString(), leftOffDate, lastContinue);
        }

        /**
         * Get continue object.
         */
        public Continue getLastContinue() {
            return lastContinue;
        }
    }

    /**
     * Check whether change is very recent.
     * If it is we need to backoff to allow capturing unsettled DB changes.
     */
    private boolean changeIsRecent(Instant nextStartTime) {
        return nextStartTime.isAfter(Instant.now().minus(BACKOFF_THRESHOLD));
    }

    /**
     * Fetch recent changes from Wikibase.
     * If we're close to current time, we back off a bit from last timestamp,
     * and fetch by timestamp. If it's back in the past, we fetch by continuation.
     *
     * @throws RetryableException on fetch failure
     */
    private RecentChangeResponse fetchRecentChanges(Instant lastNextStartTime, Batch lastBatch) throws RetryableException {
        try (Context timerContext = recentChangesTimer.time()) {
            RecentChangeResponse recentChanges = doFetchRecentChanges(lastNextStartTime, lastBatch);
            recentChangesCounter.inc(recentChanges.getQuery().getRecentChanges().size());
            return recentChanges;
        }
    }

    private RecentChangeResponse doFetchRecentChanges(Instant lastNextStartTime, Batch lastBatch) throws RetryableException {
        if (useBackoff && changeIsRecent(lastNextStartTime)) {
            return wikibase.fetchRecentChangesByTime(
                    lastNextStartTime.minus(BACKOFF_TIME), batchSize);
        } else {
            return wikibase.fetchRecentChanges(lastNextStartTime,
                    lastBatch != null ? lastBatch.getLastContinue() : null,
                    batchSize);
        }
    }

    /**
     * Parse a batch from the api result.
     *
     * @throws RetryableException on parse failure
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    private Batch batch(Instant lastNextStartTime, Batch lastBatch) throws RetryableException {
        RecentChangeResponse recentChanges = fetchRecentChanges(lastNextStartTime, lastBatch);
        // Using LinkedHashMap here so that changes came out sorted by order of arrival
        Map<String, Change> changesByTitle = new LinkedHashMap<>();
        Continue nextContinue = recentChanges.getContinue();
        Instant nextStartTime = lastNextStartTime;
        List<RecentChange> result = recentChanges.getQuery().getRecentChanges();

        for (RecentChange rc : result) {
            // Does not matter if the change matters for us or not, it
            // still advances the time since we've seen it.
            nextStartTime = Utils.max(nextStartTime, rc.getTimestamp());
            if (rc.getNs() == null) {
                LOG.warn("Skipping change without a namespace:  {}", rc);
                continue;
            }
            if (!wikibase.isEntityNamespace(rc.getNs())) {
                LOG.info("Skipping change in irrelevant namespace:  {}", rc);
                continue;
            }
            if (!wikibase.isValidEntity(rc.getTitle())) {
                LOG.info("Skipping change with bogus title:  {}", rc.getTitle());
                continue;
            }
            if (seenIDs.containsKey(rc.getRcId())) {
                // This change was in the last batch
                LOG.debug("Skipping repeated change with rcid {}", rc.getRcId());
                continue;
            }
            seenIDs.put(rc.getRcId(), TRUE);
            // Looks like we can not rely on changes appearing in order in RecentChanges,
            // so we have to take them all and let SPARQL sort out the dupes.
            Change change;
            if (rc.getType().equals("log") && rc.getRevId() == 0) {
                // Deletes should always be processed, so put negative revision
                change = new Change(rc.getTitle(), -1L, rc.getTimestamp(), rc.getRcId());
            } else {
                change = new Change(rc.getTitle(), rc.getRevId(), rc.getTimestamp(), rc.getRcId());
            }
            /*
             * Remove duplicate changes by title keeping the latest
             * revision. Note that negative revision means always update, so those
             * are kept.
             */
            Change dupe = changesByTitle.put(change.entityId(), change);
            if (dupe != null && (dupe.revision() > change.revision() || dupe.revision() < 0)) {
                // need to remove so that order will be correct
                changesByTitle.remove(change.entityId());
                changesByTitle.put(change.entityId(), dupe);
            }
        }
        final ImmutableList<Change> changes = ImmutableList.copyOf(changesByTitle.values());
        // Backoff overflow is when:
        // a. We use backoff
        // b. We got full batch of changes.
        // c. None of those were new changes.
        // In this case, sleeping and trying again is obviously useless.
        final boolean backoffOverflow = useBackoff && changes.isEmpty() && result.size() >= batchSize;
        if (backoffOverflow) {
            // We have a problem here - due to backoff, we did not fetch any new items
            // Try to advance one second, even though we risk to lose a change - in hope
            // that trailing poller will pick them up.
            nextStartTime = nextStartTime.plusSeconds(1);
            LOG.info("Backoff overflow, advancing next time to {}", nextStartTime);
        }

        if (!changes.isEmpty()) {
            LOG.info("Got {} changes, from {} to {}", changes.size(),
                    changes.get(0),
                    changes.get(changes.size() - 1));
        } else {
            LOG.info("Got no real changes");
        }

        // Show the user the polled time - one second because we can't
        // be sure we got the whole second
        long advanced = ChronoUnit.MILLIS.between(lastNextStartTime, nextStartTime);
        Batch batch = new Batch(changes, advanced, nextStartTime.minusSeconds(1).toString(), nextStartTime, nextContinue);
        if (backoffOverflow && nextContinue != null) {
            // We will not sleep if continue is provided.
            LOG.info("Got only old changes, next is: {}", nextContinue);
            batch.hasChanges(true);
        }
        return batch;
    }

    @Override
    public void close() {
        // TODO: Do we need to do anything here?
    }
}
