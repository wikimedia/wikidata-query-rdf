package org.wikidata.query.rdf.tool.change;

import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;

import java.text.DateFormat;
import java.util.Date;

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

    private final WikibaseRepository wikibase;
    private final Date firstStartTime;

    public RecentChangesPoller(WikibaseRepository wikibase, Date firstStartTime) {
        this.wikibase = wikibase;
        this.firstStartTime = firstStartTime;
    }

    @Override
    public Batch firstBatch() throws RetryableException {
        return batch(firstStartTime, null);
    }

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        return batch(lastBatch.nextStartTime, lastBatch.nextContinue);
    }

    public class Batch extends Change.Batch.AbstractDefaultImplementation {
        private final Date nextStartTime;
        private final JSONObject nextContinue;

        /**
         * A batch that will next continue using the continue parameter.
         */
        private Batch(ImmutableList<Change> changes, long advanced, String upTo, Date nextStartTime,
                JSONObject nextContinue) {
            super(changes, advanced, upTo);
            this.nextContinue = nextContinue;
            this.nextStartTime = nextStartTime;
        }

        @Override
        public String advancedUnits() {
            return "milliseconds";
        }
    }

    /**
     * Parse a batch from the api result.
     *
     * @throws RetryableException on parse failure
     */
    private Batch batch(Date lastNextStartTime, JSONObject lastNextContinue) throws RetryableException {
        try {
            JSONObject recentChanges = wikibase.fetchRecentChanges(lastNextStartTime, lastNextContinue);
            ImmutableList.Builder<Change> changes = ImmutableList.builder();
            JSONObject nextContinue = (JSONObject) recentChanges.get("continue");
            long nextStartTime = 0;
            JSONArray result = (JSONArray) ((JSONObject) recentChanges.get("query")).get("recentchanges");
            DateFormat df = inputDateFormat();
            for (Object rco : result) {
                JSONObject rc = (JSONObject) rco;
                if ((long) rc.get("ns") != 0) {
                    log.debug("Skipping change in non-0 namespace:  {}", rc);
                    continue;
                }
                Date timestamp = df.parse(rc.get("timestamp").toString());
                changes.add(new Change(rc.get("title").toString(), (long) rc.get("revid"), timestamp));
                nextStartTime = Math.max(nextStartTime, timestamp.getTime());
            }
            // Show the user the polled time - one seconds because we can't
            // be sure we got the whole second
            String upTo = inputDateFormat().format(new Date(nextStartTime - 1000));
            long advanced = nextStartTime - lastNextStartTime.getTime();
            return new Batch(changes.build(), advanced, upTo, new Date(nextStartTime), nextContinue);
        } catch (java.text.ParseException e) {
            throw new RetryableException("Parse error from api", e);
        }
    }
}
