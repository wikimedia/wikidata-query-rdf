package org.wikidata.query.rdf.tool.change;

import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;

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

    private static final Logger LOG = LoggerFactory
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
    private final BlockingQueue<Batch> queue;

    /**
     * Main poller timestamp.
     */
    private volatile Instant mainPollerTs;

    public TailingChangesPoller(RecentChangesPoller poller, BlockingQueue<Batch> queue, int tailSeconds) {
        this.poller = poller;
        this.tailSeconds = tailSeconds;
        this.queue = queue;
    }

    /**
     * Set main poller timestamp.
     * @param ts Main poller timestamp.
     */
    public void setPollerTs(Instant ts) {
        mainPollerTs = ts;
    }

    /**
     * Is this timestamp old enough?
     */
    public boolean isOldEnough(Instant timestamp) {
        return timestamp.isBefore(Instant.now().minusSeconds(tailSeconds));
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
                        LOG.warn("Retryable error fetching first batch.  Retrying.", e);
                        continue;
                    }
                } while (false);
                // Process the batch
                if (!lastBatch.changes().isEmpty()) {
                    LOG.info("Caught {} missing updates, adding to the queue", lastBatch.changes().size());
                    queue.put(lastBatch);
                }
                LOG.info("Tail poll up to {}", lastBatch.leftOffDate());
                if (mainPollerTs != null && mainPollerTs.isBefore(lastBatch.leftOffDate())) {
                    // We are ahead of main poller, this is not good, normally should not happen
                    long sleepTime = MILLIS.between(mainPollerTs, lastBatch.leftOffDate()) + tailSeconds * 1000;
                    // Waiting for sleepTime does not guarantee RC poller would catch up
                    // - we don't how long that would take - but it gives it a chance.
                    LOG.info("Got ahead of main poller ({} > {}), sleeping for {}...", lastBatch.leftOffDate(), mainPollerTs, sleepTime);
                    Thread.sleep(sleepTime);
                }
                if (!isOldEnough(lastBatch.leftOffDate())) {
                    // we're too far forward, let's sleep for a bit so we are couple
                    // of seconds behind
                    long sleepTime = MILLIS.between(lastBatch.leftOffDate(), Instant.now().plusSeconds(tailSeconds + 2));
                    LOG.info("Got too close to the current stream, sleeping for {}...", sleepTime);
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
