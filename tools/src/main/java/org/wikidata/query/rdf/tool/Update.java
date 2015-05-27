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
import java.util.List;
import java.util.Locale;
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
import org.wikidata.query.rdf.tool.change.IdChangeSource;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.Meter;
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

        @Option(shortName = "b", defaultValue = "10", description = "Number of recent changes fetched at a time.")
        int batchSize();
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
        new Update<>(changeSource, wikibaseRepository, rdfRepository, munger, executor, options.pollDelay()).run();
    }

    /**
     * Build a change source.
     *
     * @return null if non can be built - its ok to just exit - errors have been
     *         logged to the user
     */
    private static Change.Source<? extends Batch> buildChangeSource(Options options, RdfRepository rdfRepository,
            WikibaseRepository wikibaseRepository) {
        if (options.ids() != null) {
            String[] ids = options.ids().split("-");
            long start;
            long end;
            switch (ids.length) {
            case 1:
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
            return IdChangeSource.forItems(start, end, options.batchSize());
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
     * Meter for the raw number of updates synced.
     */
    private final Meter updateMeter = new Meter();
    /**
     * Meter measuring in a batch specific unit. For the RecentChangesPoller its
     * milliseconds, for the IdChangeSource its ids.
     */
    private final Meter batchAdvanced = new Meter();

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

    public Update(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
            Munger munger, ExecutorService executor, int pollDelay) {
        this.changeSource = changeSource;
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.munger = munger;
        this.executor = executor;
        this.pollDelay = pollDelay;
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
        for (final Change change : batch.changes()) {
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
        log.debug("Received revision information {}", change);
        if (change.revision() >= 0 && rdfRepository.hasRevision(change.entityId(), change.revision())) {
            log.debug("RDF repository already has this revision, skipping.");
            return;
        }
        Collection<Statement> statements = wikibase.fetchRdfForEntity(change.entityId());
        Set<String> values = rdfRepository.getValues(change.entityId());
        Set<String> refs = rdfRepository.getRefs(change.entityId());
        munger.munge(change.entityId(), statements, values, refs, change);
        List<String> cleanupList = new ArrayList<>();
        cleanupList.addAll(values);
        cleanupList.addAll(refs);
        rdfRepository.sync(change.entityId(), statements, cleanupList);
        updateMeter.mark();
    }

    /**
     * Turn a Meter into a load average style report.
     */
    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }
}
