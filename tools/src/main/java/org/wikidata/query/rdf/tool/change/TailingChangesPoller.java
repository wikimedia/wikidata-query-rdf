package org.wikidata.query.rdf.tool.change;

import java.util.Date;
import java.util.Queue;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller.Batch;
import org.wikidata.query.rdf.tool.exception.RetryableException;

/**
 * Tailing changes poller.
 * Polls updates that are certain time behind current time (to give
 * the system time to settle old updates) and if it find some, puts them
 * on the queue.
 *
 * The class tries to stay behind the updates and never catch up with the
 * current stream. In most cases, it will not produce any updates since
 * those are already collected by the main updater, but in some cases it
 * might catch update that the main one skipped.
 *
 */
public class TailingChangesPoller extends Thread {

    private static final Logger log = LoggerFactory
            .getLogger(TailingChangesPoller.class);

    /**
     * Poller to use for trailing polling.
     */
    private final RecentChangesPoller poller;
    /**
     * Last batch received from the poller.
     */
    private Batch lastBatch;
    /**
     * How far behind the current time we should keep?
     */
    private final int tailSeconds;
    /**
     * Queue to post the batches in.
     */
    private final Queue<Batch> queue;

    public TailingChangesPoller(RecentChangesPoller poller, Queue<Batch> queue, int tailSeconds) {
        this.poller = poller;
        this.tailSeconds = tailSeconds;
        this.queue = queue;
    }

    /**
     * Is this timestamp old enough?
     * @param timestamp
     * @return
     */
    public boolean isOldEnough(Date timestamp) {
        return timestamp.before(DateUtils.addSeconds(new Date(), -tailSeconds));
    }

    @Override
    public void run() {
        this.setName("TailPoller");
        while (true) {
            try {
                do {
                    try {
                        if (lastBatch == null) {
                            lastBatch = poller.firstBatch();
                        } else {
                            lastBatch = poller.nextBatch(lastBatch);
                        }
                    } catch (RetryableException e) {
                        log.warn("Retryable error fetching first batch.  Retrying.", e);
                        continue;
                    }
                } while (false);
                // Process the batch
                if (lastBatch.changes().size() > 0) {
                    log.info("Caught {} missing updates, adding to the queue", lastBatch.changes().size());
                    queue.add(lastBatch);
                }
                log.info("Tail poll up to {}", lastBatch.leftOffDate());
                if (!isOldEnough(lastBatch.leftOffDate())) {
                    // we're too far forward, let's sleep for a bit so we are couple
                    // of seconds behind
                    long sleepTime = lastBatch.leftOffDate().getTime() -
                            DateUtils.addSeconds(new Date(), -tailSeconds - 2).getTime();
                    log.info("Got too close to the current stream, sleeping for {}...", sleepTime);
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
