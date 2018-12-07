package org.wikidata.query.rdf.tool;

import static java.lang.Thread.currentThread;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.Change.DelayedChange;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

/**
 * Update tool.
 *
 * This contains the core logic of the update tool.
 *
 * @param <B> type of update batch
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class Updater<B extends Change.Batch> implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(Updater.class);

    /**
     * Meter for the raw number of updates synced.
     */
    private final Meter updatesMeter;
    /**
     * Meter measuring in a batch specific unit. For the RecentChangesPoller its
     * milliseconds, for the IdChangeSource its ids.
     */
    private final Meter batchAdvanced;
    /**
     * Source of change batches.
     */
    private final Change.Source<B> changeSource;
    /**
     * Wikibase to read rdf from.
     */
    private final WikibaseRepository wikibase;
    /**
     * Repository to which to sync rdf.
     */
    private final RdfRepository rdfRepository;
    /**
     * Munger to munge rdf from wikibase before adding it to the rdf store.
     */
    private final Munger munger;
    /**
     * The executor to use for updates.
     */
    private final ExecutorService executor;
    /**
     * Seconds to wait after we hit an empty batch. Empty batches signify that
     * there aren't any changes left now but the change stream isn't over. In
     * particular this will happen if the RecentChangesPoller finds no changes.
     */
    private final int pollDelay;
    /**
     * Uris for wikibase.
     */
    private final WikibaseUris uris;
    /**
     * Queue of delayed changes.
     * Change is delayed if RDF data produces lower revision than change - this means we're
     * affected by replication lag.
     */
    private final DelayQueue<DelayedChange> deferralQueue;

    /**
     * Map entity->values list from repository.
     */
    private ImmutableSetMultimap<String, String> repoValues;
    /**
     * Map entity->references list from repository.
     */
    private ImmutableSetMultimap<String, String> repoRefs;
    /**
     * Should we verify updates?
     */
    private final boolean verify;

    Updater(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
                   Munger munger, ExecutorService executor, int pollDelay, WikibaseUris uris, boolean verify,
                   MetricRegistry metricRegistry) {
        this.changeSource = changeSource;
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.munger = munger;
        this.executor = executor;
        this.pollDelay = pollDelay;
        this.uris = uris;
        this.verify = verify;
        this.updatesMeter = metricRegistry.meter("updates");
        this.batchAdvanced = metricRegistry.meter("batch-progress");
        this.deferralQueue = new DelayQueue<>();
    }

    @Override
    public void run() {
        B batch = null;
        do {
            try {
                batch = changeSource.firstBatch();
            } catch (RetryableException e) {
                log.warn("Retryable error fetching first batch.  Retrying.", e);
            }
        } while (batch == null);
        log.debug("{} changes in batch", batch.changes().size());
        Instant oldDate = null;
        while (!currentThread().isInterrupted()) {
            try {
                handleChanges(addDeferredChanges(deferralQueue, batch.changes()));
                Instant leftOffDate = batch.leftOffDate();
                if (leftOffDate != null) {
                    /*
                     * Back one second because the resolution on our poll isn't
                     * super good and because its not big deal to recheck if we
                     * have some updates.
                     */
                    leftOffDate = leftOffDate.minusSeconds(1);
                    // Do not update repo with the same date
                    if (oldDate == null || !oldDate.equals(leftOffDate)) {
                        syncDate(leftOffDate);
                        oldDate = leftOffDate;
                    }
                }
                // TODO wrap all retry-able exceptions in a special exception
                batchAdvanced.mark(batch.advanced());
                log.info("Polled up to {} at {} updates per second and {} {} per second", batch.leftOffHuman(),
                        meterReport(updatesMeter), meterReport(batchAdvanced), batch.advancedUnits());
                if (batch.last()) {
                    return;
                }
                wikibase.batchDone();
                batch = nextBatch(batch);
            } catch (InterruptedException e) {
                currentThread().interrupt();
            }
        }
    }

    /**
     * If there are any deferred changes, add them to changes list.
     * @return Modified collection as iterable.
     */
    @VisibleForTesting
    static Collection<Change> addDeferredChanges(DelayQueue<DelayedChange> deferralQueue, Collection<Change> newChanges) {
        if (deferralQueue.isEmpty()) {
            return newChanges;
        }
        List<Change> allChanges = new LinkedList<>(newChanges);
        int deferrals = 0;
        Set<String> changeIds = newChanges.stream().map(Change::entityId).collect(ImmutableSet.toImmutableSet());
        for (DelayedChange deferred = deferralQueue.poll();
             deferred != null;
             deferred = deferralQueue.poll()) {
           if (changeIds.contains(deferred.getChange().entityId())) {
               // This title ID already has newer change, drop the deferral.
               // NOTE: here we assume that incoming stream always has changes in order of increasing revisions,
               // which sounds plausible.
               continue;
           }
            allChanges.add(deferred.getChange());
           deferrals++;
        }
        log.info("Added {} deferred changes", deferrals);
        return allChanges;
    }

    /**
     * Record that we reached certain date in permanent storage.
     */
    protected void syncDate(Instant newDate) {
        rdfRepository.updateLeftOffTime(newDate);
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        changeSource.close();
    }

    /**
     * Handle the changes in a batch.
     *
     * @throws InterruptedException if the process is interrupted while waiting
     *             on changes to sync
     */
    protected void handleChanges(Iterable<Change> changes) throws InterruptedException {
        Set<Change> trueChanges = getRevisionUpdates(changes);
        long start = System.currentTimeMillis();

        List<Future<Change>> futureChanges = new ArrayList<>();
        for (Change change : trueChanges) {
            futureChanges.add(executor.submit(() -> {
                while (true) {
                    try {
                        handleChange(change);
                        return change;
                    } catch (RetryableException e) {
                        log.warn("Retryable error syncing.  Retrying.", e);
                    } catch (ContainedException e) {
                        log.warn("Contained error syncing.  Giving up on {}", change.entityId(), e);
                        throw e;
                    }
                }
            }));
        }

        List<Change> processedChanges = new ArrayList<>();
        for (Future<Change> f : futureChanges) {
            try {
                processedChanges.add(f.get());
            } catch (ExecutionException ignore) {
                // failure has already been logged
            }
        }

        log.debug("Preparing update data took {} ms, have {} changes", System.currentTimeMillis() - start, processedChanges.size());
        rdfRepository.syncFromChanges(processedChanges, verify);
        updatesMeter.mark(processedChanges.size());
    }

    /**
     * Filter change by revisions.
     * The revisions that have the same or superior revision in the DB will be removed.
     * @param changes Collection of incoming changes.
     * @return A set of changes that need to be entered into the repository.
     */
    private Set<Change> getRevisionUpdates(Iterable<Change> changes) {
        // List of changes that indeed need update
        Set<Change> trueChanges = new HashSet<>();
        // List of entity URIs that were changed
        Set<String> changeIds = new HashSet<>();
        Map<String, Change> candidateChanges = new HashMap<>();
        for (final Change change : changes) {
            if (change.revision() > Change.NO_REVISION) {
                Change c = candidateChanges.get(change.entityId());
                if (c == null || c.revision() < change.revision()) {
                    candidateChanges.put(change.entityId(), change);
                }
            } else {
                trueChanges.add(change);
                changeIds.add(uris.entity() + change.entityId());
            }
        }
        if (candidateChanges.size() > 0) {
            for (String entityId: rdfRepository.hasRevisions(candidateChanges.values())) {
                // Cut off the entity prefix from the resulting URI
                changeIds.add(entityId);
                trueChanges.add(candidateChanges.get(entityId.substring(uris.entity().length())));
            }
        }
        log.debug("Filtered batch contains {} changes", trueChanges.size());

        if (trueChanges.size() > 0) {
            setValuesAndRefs(
                    rdfRepository.getValues(changeIds),
                    rdfRepository.getRefs(changeIds)
            );
            if (log.isDebugEnabled()) {
                synchronized (this) {
                    log.debug("Fetched {} values", repoValues.size());
                    log.debug("Fetched {} refs", repoRefs.size());
                }
            }
        } else {
            setValuesAndRefs(null, null);
        }

        return trueChanges;
    }

    private synchronized void setValuesAndRefs(
            ImmutableSetMultimap<String, String> values,
            ImmutableSetMultimap<String, String> refs) {
        repoValues = values;
        repoRefs = refs;
    }

    /**
     * Fetch the next batch.
     *
     * @throws InterruptedException if the process was interrupted while waiting
     *             during the pollDelay or waiting on something else
     */
    private B nextBatch(B prevBatch) throws InterruptedException {
        B batch;
        while (true) {
            try {
                batch = changeSource.nextBatch(prevBatch);
            } catch (RetryableException e) {
                log.warn("Retryable error fetching next batch.  Retrying.", e);
                continue;
            }
            if (!batch.hasAnyChanges()) {
                log.info("Sleeping for {} secs", pollDelay);
                Thread.sleep(pollDelay * 1000);
                continue;
            }
            if (batch.changes().isEmpty()) {
                prevBatch = batch;
                continue;
            }
            log.debug("{} changes in batch", batch.changes().size());
            return batch;
        }
    }

    /**
     * Handle a change.
     * <ul>
     * <li>Check if the RDF store has the version of the page.
     * <li>Fetch the RDF from the Wikibase install.
     * <li>Add revision information to the statements if it isn't there already.
     * <li>Sync data to the triple store.
     * </ul>
     *
     * @throws RetryableException if there is a retryable error updating the rdf
     *             store
     */
    private void handleChange(Change change) throws RetryableException {
        log.debug("Processing data for {}", change);
        Collection<Statement> statements = wikibase.fetchRdfForEntity(change.entityId());
        Set<String> values = new HashSet<>();
        Set<String> refs = new HashSet<>();
        ImmutableSetMultimap<String, String> repoValues;
        ImmutableSetMultimap<String, String> repoRefs;
        synchronized (this) {
            repoValues = this.repoValues;
            repoRefs = this.repoRefs;
        }
        munger.mungeWithValues(change.entityId(), statements, repoValues, repoRefs, values, refs, change, deferralQueue);
        change.setRefCleanupList(refs);
        change.setValueCleanupList(values);
        change.setStatements(statements);
    }

    /**
     * Turn a Meter into a load average style report.
     */
    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }
}
