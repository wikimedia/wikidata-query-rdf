package org.wikidata.query.rdf.tool;

import static java.lang.Thread.currentThread;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
public class Updater<B extends Change.Batch> implements Runnable, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Updater.class);

    /**
     * Metric registry.
     */
    private final MetricRegistry metrics = new MetricRegistry();
    /**
     * Meter for the raw number of updates synced.
     */
    private final Meter updateMeter = metrics.meter("updates");
    /**
     * Meter measuring in a batch specific unit. For the RecentChangesPoller its
     * milliseconds, for the IdChangeSource its ids.
     */
    private final Meter batchAdvanced = metrics.meter("batch-progress");
    /**
     * JMX interface for metrics counters.
     */
    private final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
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
     * Map entity->values list from repository.
     */
    private volatile ImmutableSetMultimap<String, String> repoValues;
    /**
     * Map entity->references list from repository.
     */
    private volatile ImmutableSetMultimap<String, String> repoRefs;
    /**
     * Should we verify updates?
     */
    private final boolean verify;

    public Updater(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
                   Munger munger, ExecutorService executor, int pollDelay, WikibaseUris uris, boolean verify) {
        this.changeSource = changeSource;
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.munger = munger;
        this.executor = executor;
        this.pollDelay = pollDelay;
        this.uris = uris;
        this.verify = verify;
        reporter.start();
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
                handleChanges(batch.changes());
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
                        meterReport(updateMeter), meterReport(batchAdvanced), batch.advancedUnits());
                if (batch.last()) {
                    return;
                }
                batch = nextBatch(batch);
            } catch (InterruptedException e) {
                currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Syncing encountered a fatal exception", e);
                break;
            }
        }
    }

    /**
     * Record that we reached certain date in permanent storage.
     */
    protected void syncDate(Instant newDate) {
        rdfRepository.updateLeftOffTime(newDate);
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
        changeSource.close();
    }

    /**
     * Handle the changes in a batch.
     *
     * @throws InterruptedException if the process is interrupted while waiting
     *             on changes to sync
     * @throws ExecutionException if there is an error syncing any of the
     *             changes
     */
    protected void handleChanges(Iterable<Change> changes) throws InterruptedException, ExecutionException {
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
        updateMeter.mark(processedChanges.size());
    }

    /**
     * Filter change by revisions.
     * The revisions that have the same or superior revision in the DB will be removed.
     * @return A set of changes that need to be entered into the repository.
     * @param changes
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
            repoValues = rdfRepository.getValues(changeIds);
            log.debug("Fetched {} values", repoValues.size());
            repoRefs = rdfRepository.getRefs(changeIds);
            log.debug("Fetched {} refs", repoRefs.size());
        } else {
            repoValues = null;
            repoRefs = null;
        }

        return trueChanges;
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
        munger.mungeWithValues(change.entityId(), statements, repoValues, repoRefs, values, refs, change);
        List<String> cleanupList = new ArrayList<>();
        cleanupList.addAll(values);
        cleanupList.addAll(refs);
        change.setStatements(statements);
        change.setCleanupList(cleanupList);
    }

    /**
     * Turn a Meter into a load average style report.
     */
    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }
}
