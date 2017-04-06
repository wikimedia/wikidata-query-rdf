package org.wikidata.query.rdf.tool.change;

import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.google.common.collect.ImmutableList;

/**
 * Polls recent changes. The first batch polls the recent changes api starting
 * at the firstStartTime. Subsequent batches either start on the last continue
 * of the previous poll, or, if there isn't a continue, then they start one
 * second after the last first start time.
 */
public class RecentChangesPoller implements Change.Source<RecentChangesPoller.Batch> {
    private static final Logger log = LoggerFactory.getLogger(RecentChangesPoller.class);

    /**
     * Wikibase repository to poll.
     */
    private final WikibaseRepository wikibase;
    /**
     * The first start time to poll.
     */
    private final Date firstStartTime;
    /**
     * Size of the batches to poll against wikibase.
     */
    private final int batchSize;
    /**
     * How much to back off for recent fetches, in seconds.
     */
    private static final int BACKOFF_TIME = 10;
    /**
     * How old should the change be to not apply backoff.
     * The number is in minutes.
     */
    private static final int BACKOFF_THRESHOLD = 5;

    public RecentChangesPoller(WikibaseRepository wikibase, Date firstStartTime, int batchSize) {
        this.wikibase = wikibase;
        this.firstStartTime = firstStartTime;
        this.batchSize = batchSize;
    }

    @Override
    public Batch firstBatch() throws RetryableException {
        return batch(firstStartTime, null);
    }

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        return batch(lastBatch.leftOffDate, lastBatch);
    }

    /**
     * Batch implementation for this poller.
     */
    public final class Batch extends Change.Batch.AbstractDefaultImplementation {
        /**
         * The date where we last left off.
         */
        private final Date leftOffDate;

        /**
         * The set of the rcid's this batch seen.
         * Note that some IDs may be seen but not processed
         * due to duplicates, etc.
         */
        private final Set<Long> seenIDs;

        /**
         * Continue from last request. Can be null.
         */
        private final JSONObject lastContinue;

        /**
         * A batch that will next continue using the continue parameter.
         */
        private Batch(ImmutableList<Change> changes, long advanced,
                String leftOff, Date nextStartTime, JSONObject lastContinue,
                Set<Long> seenIDs) {
            super(changes, advanced, leftOff);
            leftOffDate = nextStartTime;
            this.seenIDs = seenIDs;
            this.lastContinue = lastContinue;
        }

        @Override
        public String advancedUnits() {
            return "milliseconds";
        }

        @Override
        public Date leftOffDate() {
            return leftOffDate;
        }

        @Override
        public String leftOffHuman() {
            return WikibaseRepository.inputDateFormat().format(leftOffDate)
                + " (next: " + lastContinue.get("rccontinue").toString() + ")";
        }

        /**
         * Get the list of IDs this batch has seen.
         * @return
         */
        public Set<Long> getSeenIDs() {
            return seenIDs;
        }

        /**
         * Get continue object.
         * @return
         */
        public JSONObject getLastContinue() {
            return lastContinue;
        }
    }

    /**
     * Check whether change is very recent.
     * If it is we need to backoff to allow capturing unsettled DB changes.
     * @param nextStartTime
     * @return
     */
    private boolean changeIsRecent(Date nextStartTime) {
        return nextStartTime.after(DateUtils.addMinutes(new Date(), -BACKOFF_THRESHOLD));
    }

    /**
     * Fetch recent changes from Wikibase.
     * If we're close to current time, we back off a bit from last timestamp,
     * and fetch by timestamp. If it's back in the past, we fetch by continuation.
     * @param lastNextStartTime
     * @param lastBatch
     * @return
     * @throws RetryableException on fetch failure
     */
    private JSONObject fetchRecentChanges(Date lastNextStartTime, Batch lastBatch) throws RetryableException {
        if (changeIsRecent(lastNextStartTime)) {
            return wikibase.fetchRecentChangesByTime(
                    DateUtils.addSeconds(lastNextStartTime, -BACKOFF_TIME),
                    batchSize);
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
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private Batch batch(Date lastNextStartTime, Batch lastBatch) throws RetryableException {
        try {
            JSONObject recentChanges = fetchRecentChanges(lastNextStartTime, lastBatch);
            // Using LinkedHashMap here so that changes came out sorted by order of arrival
            Map<String, Change> changesByTitle = new LinkedHashMap<>();
            JSONObject nextContinue = (JSONObject) recentChanges.get("continue");
            long nextStartTime = lastNextStartTime.getTime();
            JSONArray result = (JSONArray) ((JSONObject) recentChanges.get("query")).get("recentchanges");
            DateFormat df = inputDateFormat();
            Set<Long> seenIds = new HashSet<>();

            for (Object rco : result) {
                JSONObject rc = (JSONObject) rco;
                long namespace = (long) rc.get("ns");
                long rcid = (long)rc.get("rcid");
                if (!wikibase.isEntityNamespace(namespace)) {
                    log.info("Skipping change in irrelevant namespace:  {}", rc);
                    continue;
                }
                if (!wikibase.isValidEntity(rc.get("title").toString())) {
                    log.info("Skipping change with bogus title:  {}", rc.get("title").toString());
                    continue;
                }
                seenIds.add(rcid);
                if (lastBatch != null && lastBatch.getSeenIDs().contains(rcid)) {
                    // This change was in the last batch
                    log.debug("Skipping repeated change with rcid {}", rcid);
                    continue;
                }
// Looks like we can not rely on changes appearing in order, so we have to take them all and let SPARQL
// sort out the dupes.
//                if (continueChange != null && rcid < continueChange.rcid()) {
//                    // We've already seen this change, since it has older rcid - so skip it
//                    continue;
//                }
                Date timestamp = df.parse(rc.get("timestamp").toString());
                Change change;
                if (rc.get("type").toString().equals("log") && (long)rc.get("revid") == 0) {
                    // Deletes should always be processed, so put negative revision
                    change = new Change(rc.get("title").toString(), -1L, timestamp, rcid);
                } else {
                    change = new Change(rc.get("title").toString(), (long) rc.get("revid"), timestamp, (long)rc.get("rcid"));
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
                nextStartTime = Math.max(nextStartTime, timestamp.getTime());
            }
            ImmutableList<Change> changes = ImmutableList.copyOf(changesByTitle.values());
            if (changes.size() == 0 && result.size() >= batchSize) {
                // We have a problem here - due to backoff, we did not fetch any new items
                // Try to advance one second, even though we risk to lose a change
                log.info("Backoff overflow, advancing one second");
                nextStartTime += 1000;
            }

            if (changes.size() != 0) {
                log.info("Got {} changes, from {} to {}", changes.size(),
                        changes.get(0).toString(),
                        changes.get(changes.size() - 1).toString());
            } else {
                log.info("Got no real changes");
            }

            // Show the user the polled time - one second because we can't
            // be sure we got the whole second
            String upTo = inputDateFormat().format(new Date(nextStartTime - 1000));
            long advanced = nextStartTime - lastNextStartTime.getTime();
            return new Batch(changes, advanced, upTo, new Date(nextStartTime), nextContinue, seenIds);
        } catch (java.text.ParseException e) {
            throw new RetryableException("Parse error from api", e);
        }
    }
}
