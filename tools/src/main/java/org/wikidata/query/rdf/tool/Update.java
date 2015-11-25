package org.wikidata.query.rdf.tool;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.wikidata.query.rdf.tool.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.OptionsUtils.mungerFromOptions;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.OptionsUtils.BasicOptions;
import org.wikidata.query.rdf.tool.OptionsUtils.MungerOptions;
import org.wikidata.query.rdf.tool.OptionsUtils.WikibaseOptions;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.Change.Batch;
import org.wikidata.query.rdf.tool.change.IdListChangeSource;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lexicalscope.jewel.cli.Option;

/**
 * Update tool.
 *
 * @param <B> type of update batch
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class Update<B extends Change.Batch> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Update.class);

    /**
     * CLI options for use with JewelCli.
     */
    @SuppressWarnings("checkstyle:javadocmethod")
    public interface Options extends BasicOptions, MungerOptions, WikibaseOptions {
        @Option(defaultValue = "https", description = "Wikidata url scheme")
        String wikibaseScheme();

        @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
        String start();

        @Option(defaultToNull = true, description = "If specified must be <id> or <start>-<end>. Ids are iterated instead of recent "
                + "changes. Start and end are inclusive.")
        String ids();

        @Option(shortName = "u", description = "URL to post updates and queries.")
        String sparqlUrl();

        @Option(shortName = "d", defaultValue = "10", description = "Poll delay when no updates found")
        int pollDelay();

        @Option(shortName = "t", defaultValue = "10", description = "Thread count")
        int threadCount();

        @Option(shortName = "b", defaultValue = "100", description = "Number of recent changes fetched at a time.")
        int batchSize();

        @Option(shortName = "V", longName = "verify", description = "Verify updates (may have performance impact)")
        boolean verify();
    }

    /**
     * Run updates configured from the command line.
     */
    public static void main(String[] args) {
        Options options = handleOptions(Options.class, args);
        WikibaseRepository wikibaseRepository = new WikibaseRepository(options.wikibaseScheme(), options.wikibaseHost());
        URI sparqlUri;
        try {
            sparqlUri = new URI(options.sparqlUrl());
        } catch (URISyntaxException e) {
            log.error("Invalid url:  " + options.sparqlUrl() + " caused by " + e.getMessage());
            return;
        }
        WikibaseUris uris = new WikibaseUris(options.wikibaseHost());
        RdfRepository rdfRepository = new RdfRepository(sparqlUri, uris);
        Change.Source<? extends Change.Batch> changeSource = buildChangeSource(options, rdfRepository,
                wikibaseRepository);
        if (changeSource == null) {
            return;
        }
        int threads = options.threadCount();
        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("update %s");
        ExecutorService executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory.build());

        Munger munger = mungerFromOptions(options);
        new Update<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                options.pollDelay(), uris, options.verify()).run();
    }

    /**
     * Build a change source.
     *
     * @return null if non can be built - its ok to just exit - errors have been
     *         logged to the user
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private static Change.Source<? extends Batch> buildChangeSource(Options options, RdfRepository rdfRepository,
            WikibaseRepository wikibaseRepository) {
        if (options.ids() != null) {
            if (options.ids().contains(",")) {
                // Id list
                return new IdListChangeSource(options.ids().split(","), options.batchSize());
            }
            String[] ids = options.ids().split("-");
            long start;
            long end;
            switch (ids.length) {
            case 1:
                if (!Character.isDigit(ids[0].charAt(0))) {
                    // Not a digit - probably just single ID
                    return new IdListChangeSource(ids, options.batchSize());
                }
                start = Long.parseLong(ids[0]);
                end = start;
                break;
            case 2:
                start = Long.parseLong(ids[0]);
                end = Long.parseLong(ids[1]);
                break;
            default:
                log.error("Invalid format for --ids.  Need <start>-<stop>.");
                return null;
            }
            return IdRangeChangeSource.forItems(start, end, options.batchSize());
        }
        long startTime;
        if (options.start() != null) {
            try {
                startTime = outputDateFormat().parse(options.start()).getTime();
            } catch (java.text.ParseException e) {
                try {
                    startTime = inputDateFormat().parse(options.start()).getTime();
                } catch (java.text.ParseException e2) {
                    log.error("Invalid date:  {}", options.start());
                    return null;
                }
            }
        } else {
            log.info("Checking where we left off");
            Date leftOff = rdfRepository.fetchLeftOffTime();
            long minStartTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
            if (leftOff == null) {
                startTime = minStartTime;
                log.info("Defaulting start time to 30 days ago:  {}", inputDateFormat().format(new Date(startTime)));
            } else {
                if (leftOff.getTime() < minStartTime) {
                    log.error("RDF store reports the last update time is before the minimum safe poll time.  "
                            + "You will have to reload from scratch or you might have missing data.");
                    return null;
                }
                /*
                 * -2 seconds to because our precision is only 1 second and
                 * because it should be cheap to recheck that we have the right
                 * revision.
                 */
                startTime = leftOff.getTime();
                log.info("Found start time in the RDF store: {}", inputDateFormat().format(leftOff));
            }
        }
        return new RecentChangesPoller(wikibaseRepository, new Date(startTime), options.batchSize());
    }

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
    private Multimap<String, String> repoValues;
    /**
     * Map entity->references list from repository.
     */
    private Multimap<String, String> repoRefs;
    /**
     * Should we verify updates?
     */
    private final boolean verify;

    public Update(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
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
        while (true) {
            try {
                handleChanges(batch);
                Date leftOffDate = batch.leftOffDate();
                if (leftOffDate != null) {
                    /*
                     * Back one second because the resolution on our poll isn't
                     * super good and because its not big deal to recheck if we
                     * have some updates.
                     */
                    leftOffDate = new Date(batch.leftOffDate().getTime() - SECONDS.toMillis(1));
                    rdfRepository.updateLeftOffTime(leftOffDate);
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
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Syncing encountered a fatal exception", e);
                break;
            }
        }
    }

    /**
     * Handle the changes in a batch.
     *
     * @throws InterruptedException if the process is interrupted while waiting
     *             on changes to sync
     * @throws ExecutionException if there is an error syncing any of the
     *             changes
     */
    private void handleChanges(Change.Batch batch) throws InterruptedException, ExecutionException {
        List<Future<?>> tasks = new ArrayList<>();
        Set<Change> trueChanges = getRevisionUpdates(batch);
        long start = System.currentTimeMillis();
        for (final Change change : trueChanges) {
            tasks.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            handleChange(change);
                            return;
                        } catch (RetryableException e) {
                            log.warn("Retryable error syncing.  Retrying.", e);
                        } catch (ContainedException e) {
                            log.warn("Contained error syncing.  Giving up on " + change.entityId(), e);
                            return;
                        }
                    }
                }
            }));
        }

        for (Future<?> task : tasks) {
            task.get();
        }
        log.debug("Preparing update data took {} ms", System.currentTimeMillis() - start);
        rdfRepository.syncFromChanges(trueChanges, verify);
        updateMeter.mark(trueChanges.size());
    }

    /**
     * Filter change by revisions.
     * The revisions that have the same or superior revision in the DB will be removed.
     * @param batch
     * @return A set of changes that need to be entered into the repository.
     */
    private Set<Change> getRevisionUpdates(Change.Batch batch) {
        // List of changes that indeed need update
        Set<Change> trueChanges = new HashSet<>();
        // List of entity URIs that were changed
        Set<String> changeIds = new HashSet<>();
        Map<String, Change> candidateChanges = new HashMap<>();
        for (final Change change : batch.changes()) {
            if (change.revision() >= 0) {
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
    private B nextBatch(B batch) throws InterruptedException {
        while (true) {
            try {
                batch = changeSource.nextBatch(batch);
            } catch (RetryableException e) {
                log.warn("Retryable error fetching next batch.  Retrying.", e);
                continue;
            }
            if (batch.changes().isEmpty()) {
                log.debug("Sleeping for {} secs", pollDelay);
                Thread.sleep(pollDelay * 1000);
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
        Set<String> values = new HashSet<>(repoValues.get(change.entityId()));
        Set<String> refs = new HashSet<>(repoRefs.get(change.entityId()));
        munger.munge(change.entityId(), statements, values, refs, change);
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
