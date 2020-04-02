package org.wikidata.query.rdf.tool;

import static java.lang.Thread.currentThread;
import static org.wikidata.query.rdf.tool.rdf.EntityStatementsWithoutRank.entityStatementsWithoutRank;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
     * For how long (seconds) we should defer a change in case we detect replication lag.
     */
    private static final long DEFERRAL_DELAY = 5;
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
     * Blocking queue to import batches.
     */
    private final BlockingQueue<Runnable> importQueue = new ArrayBlockingQueue<>(5);
    /**
     * Whether or not we run importToRdfRepository asynchronously, allowing to prepare
     * the next batch concurrently.
     */
    private final boolean importAsync;
    /**
     * Seconds to wait after we hit an empty batch. Empty batches signify that
     * there aren't any changes left now but the change stream isn't over. In
     * particular this will happen if the RecentChangesPoller finds no changes.
     */
    private final int pollDelay;
    /**
     * Uris for wikibase.
     */
    private final UrisScheme uris;
    /**
     * Queue of delayed changes.
     * Change is delayed if RDF data produces lower revision than change - this means we're
     * affected by replication lag.
     */
    private final DeferredChanges deferredChanges = new DeferredChanges();
    /**
     * Should we verify updates?
     */
    private final boolean verify;
    /**
     * Handles recording of all the metrics related to Updater.
     */
    private final UpdaterMetricsRepository metricsRepository;

    /**
     * Last known instant we updated the repository.
     */
    private Instant lastRepoDate;

    /**
     * Thread created to run imports when importAsync is true.
     */
    private Thread importerThread;

    Updater(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
            Munger munger, ExecutorService executor, boolean importAsync, int pollDelay, UrisScheme uris, boolean verify,
            MetricRegistry metricRegistry) {
        this.changeSource = changeSource;
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.munger = munger;
        this.executor = executor;
        this.importAsync = importAsync;
        this.pollDelay = pollDelay;
        this.uris = uris;
        this.verify = verify;
        this.metricsRepository = new UpdaterMetricsRepository(metricRegistry);

    }

    @Override
    public void run() {
        if (importAsync) {
            startImporter();
        }
        try {
            B batch = null;
            do {
                try {
                    batch = changeSource.firstBatch();
                } catch (RetryableException e) {
                    log.warn("Retryable error fetching first batch.  Retrying.", e);
                }
            } while (batch == null);
            log.debug("{} changes in batch", batch.changes().size());
            while (!currentThread().isInterrupted()) {
                applyBatch(batch);
                if (batch.last()) {
                    return;
                }
                batch = nextBatch(batch);
            }
        } catch (InterruptedException ie) {
            currentThread().interrupt();
        } finally {
            if (importAsync) {
                importerThread.interrupt();
            }
        }
    }

    private void applyBatch(B batch) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        handleChanges(this.deferredChanges.augmentWithDeferredChanges(batch.changes()),
                () -> {
                    // we guarantee proper ordering here only because we can import one
                    // and only one batch at a time (see startImporter).
                    Instant leftOffDate = batch.leftOffDate();
                    if (leftOffDate != null) {
                        syncDate(leftOffDate);
                    }
                    metricsRepository.markBatchProgress(batch.advanced());
                    log.info("Polled up to {} at {} updates per second and {} {} per second", batch.leftOffHuman(),
                            metricsRepository.reportUpdatesMeter(), metricsRepository.reportBatchProgress(), batch.advancedUnits());
                    changeSource.done(batch);
                    countDownLatch.countDown();
                    if (batch.last()) {
                        return;
                    }
                    wikibase.batchDone();
                });
        if (batch.last()) {
            // Wait for completion if this is the last batch.
            if (importAsync) {
                while (!countDownLatch.await(1, TimeUnit.SECONDS)) {
                    checkImporterAlive();
                }
            }
        }
    }

    private void checkImporterAlive() {
        if (!importerThread.isAlive()) {
            throw new RuntimeException("Imported thread died, cannot continue");
        }
    }

    private void startImporter() {
        if (importerThread != null) {
            throw new IllegalStateException("Importer thread already created");
        }
        importerThread = new Thread(() -> {
            try {
                while (!Thread.interrupted()) {
                    Runnable importJob = importQueue.take();
                    importJob.run();
                }
            } catch (InterruptedException ie) {
            }
        }, "Importer");
        importerThread.setUncaughtExceptionHandler((thread, throwable) -> log.error("Importer error", throwable));
        importerThread.start();
    }

    /**
     * Record that we reached certain date in permanent storage.
     */
    protected synchronized void syncDate(Instant newDate) {
        if (lastRepoDate == null || newDate.isAfter(lastRepoDate)) {
            /*
             * Back one second because the resolution on our poll isn't
             * super good and because its not big deal to recheck if we
             * have some updates.
             */
            rdfRepository.updateLeftOffTime(newDate.minusSeconds(1));
            lastRepoDate = newDate;
        }
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
    protected void handleChanges(Collection<Change> changes, Runnable onCompleteListener) throws InterruptedException {
        ChangesWithValuesAndRefs changesWithValuesAndRefs = metricsRepository.timeRdfFetch(() -> getRevisionUpdates(changes));
        metricsRepository.incNoopedChangesByRevisionCheck(changes.size() - changesWithValuesAndRefs.changes.size());

        List<Change> processedChanges = metricsRepository.timeWikibaseDataFetch(() -> fetchDataFromWikibaseAndMunge(changesWithValuesAndRefs));

        Runnable importFunction = () -> importToRdfRepository(processedChanges, onCompleteListener);
        if (importAsync) {
            // will block until a slot is available in the import queue
            while (!this.importQueue.offer(importFunction, 1, TimeUnit.SECONDS)) {
                // Or fail if the importer job is dead
                checkImporterAlive();
            }
        } else {
            importFunction.run();
        }
    }

    private void importToRdfRepository(List<Change> processedChanges, Runnable onCompleteListener) {
        metricsRepository.updateChanges(() -> rdfRepository.syncFromChanges(processedChanges, verify), processedChanges.size());
        onCompleteListener.run();
    }

    private List<Change> fetchDataFromWikibaseAndMunge(ChangesWithValuesAndRefs trueChanges) throws InterruptedException {
        List<Future<Change>> futureChanges = new ArrayList<>();
        for (Change change : trueChanges.changes) {
            futureChanges.add(executor.submit(() -> {
                while (true) {
                    try {
                        String entityURI = uris.entityIdToURI(change.entityId());
                        Set<String> existingValues = trueChanges.repoValues.get(entityURI);
                        Set<String> existingRefs = trueChanges.repoRefs.get(entityURI);
                        handleChange(change, existingValues, existingRefs);
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

        List<Change> processedChanges = new ArrayList<>(futureChanges.size());
        for (Future<Change> f : futureChanges) {
            try {
                processedChanges.add(f.get());
            } catch (ExecutionException ignore) {
                // failure has already been logged
            }
        }
        return processedChanges;
    }

    /**
     * Filter change by revisions.
     * The revisions that have the same or superior revision in the DB will be removed.
     * @param changes Collection of incoming changes.
     * @return A set of changes that need to be entered into the repository.
     */
    @SuppressFBWarnings(value = {"UC_USELESS_OBJECT", "WOC_WRITE_ONLY_COLLECTION_LOCAL"}, justification = "see comment about T249196")
    private ChangesWithValuesAndRefs getRevisionUpdates(Iterable<Change> changes) {
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
                changeIds.add(uris.entityIdToURI(change.entityId()));
            }
        }
        if (candidateChanges.size() > 0) {
            for (String entityId: rdfRepository.hasRevisions(candidateChanges.values())) {
                // Cut off the entity prefix from the resulting URI
                changeIds.add(entityId);
                trueChanges.add(candidateChanges.get(uris.entityURItoId(entityId)));
            }
        }
        log.debug("Filtered batch contains {} changes", trueChanges.size());

        if (!trueChanges.isEmpty()) {
            /**
             * FIXME: either completely remove this feature or improve.
             *  disabled see T249196
            ImmutableSetMultimap<String, String> values = rdfRepository.getValues(changeIds);
            ImmutableSetMultimap<String, String> refs = rdfRepository.getRefs(changeIds);
             */
            ImmutableSetMultimap<String, String> values = ImmutableSetMultimap.of();
            ImmutableSetMultimap<String, String> refs = ImmutableSetMultimap.of();
            if (log.isDebugEnabled()) {
                log.debug("Fetched {} values", values.size());
                log.debug("Fetched {} refs", refs.size());
            }
            return new ChangesWithValuesAndRefs(trueChanges, values, refs);
        }

        return new ChangesWithValuesAndRefs(trueChanges, ImmutableSetMultimap.of(), ImmutableSetMultimap.of());
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
    private void handleChange(Change change, Set<String> repoValues, Set<String> repoRefs) throws RetryableException {
        log.debug("Processing data for {}", change);
        Collection<Statement> statements = wikibase.fetchRdfForEntity(change);
        if (verify) {
            Set<String> entityStmtsWithoutRank = statements
                    .stream()
                    .collect(entityStatementsWithoutRank());
            if (!entityStmtsWithoutRank.isEmpty()) {
                log.warn("Found some statements without ranks while processing {}: {}", change.entityId(), entityStmtsWithoutRank);
            }
        }
        Set<String> valuesToClean = Collections.emptySet();
        Set<String> referencesToClean = Collections.emptySet();
        if (!statements.isEmpty()) {
            valuesToClean = RdfRepository.extractValuesToCleanup(repoValues, statements);
            referencesToClean = RdfRepository.extractReferencesToCleanup(repoRefs, statements);
            long fetchedRev = munger.munge(change.entityId(), statements);

            // If we've got no statements, we have no usable loaded data, so no point in checking
            // Same if we just got back our own change - no point in checking against it
            final long sourceRev = change.revision();
            if (sourceRev > 0 && fetchedRev > 0) {
                if (fetchedRev < sourceRev) {
                    // Something weird happened - we've got stale revision!
                    log.warn("Stale revision on {}: change is {}, RDF is {}", change.entityId(), sourceRev, fetchedRev);
                    metricsRepository.incDeferredChanges();
                    deferredChanges.add(change, DEFERRAL_DELAY);
                }
                if (sourceRev < fetchedRev) {
                    // We skipped some revisions, let's count it in meter
                    metricsRepository.markSkipAhead();
                }
            }
        }

        /*
         * TODO: we temporarily keep all the ref data because of the issues
         * in https://phabricator.wikimedia.org/T194325
         * see Change-ID Ia6c68a5b93e8c9a35310892904819c956ca9cd95
         * or git commit hash 2931b5af725b7ab341dd60920710619fa249d1f2
         * for more context
         */
        referencesToClean = Collections.emptySet();
        change.setRefCleanupList(referencesToClean);
        /*
         * TODO: we disable values cleanup to measure the impact on the lag
         *  see: T249196
         */
        valuesToClean = Collections.emptySet();
        change.setValueCleanupList(valuesToClean);
        change.setStatements(statements);
    }

    /**
     * Turn a Meter into a load average style report.
     */
    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }

    public static class ChangesWithValuesAndRefs {
        private final Set<Change> changes;
        private final SetMultimap<String, String> repoValues;
        private final SetMultimap<String, String> repoRefs;

        public ChangesWithValuesAndRefs(Set<Change> changes, SetMultimap<String, String> repoValues, SetMultimap<String, String> repoRefs) {
            this.changes = changes;
            this.repoValues = repoValues;
            this.repoRefs = repoRefs;
        }
    }
}
