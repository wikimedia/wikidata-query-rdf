package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.Change.Batch;
import org.wikidata.query.rdf.tool.change.IdChangeSource;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.Cli;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.Option;

/**
 * Update tool.
 */
public class Update<B extends Change.Batch> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Update.class);

    /**
     * CLI options for use with JewelCli.
     */
    public static interface Options {
        @Option(shortName = "w", defaultValue = "www.wikidata.org", description = "Wikidata host")
        String wikidataHost();

        @Option(defaultValue = "https", description = "Wikidata url schema")
        String wikidataSchema();

        @Option(shortName = "v")
        boolean verbose();

        @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
        String start();

        @Option(defaultToNull = true, description = "If specified must be <start>-<end>. Ids are iterated instead of recent changes. Start and end are inclusive.")
        String ids();

        @Option(shortName = "u", description = "URL to post updates and queries.")
        String sparqlUrl();

        @Option(longName = "labelLanguage", defaultToNull = true, description = "Only import labels, aliases, and descriptions in these languages.")
        List<String> labelLanguages();

        @Option(longName = "singleLabel", defaultToNull = true, description = "Always import a single label and description using the languages specified as a fallback list.  If there aren't any matching labels or descriptions them the entity itself is used as the label or description.")
        List<String> singleLabelLanguages();

        @Option(description = "Skip site links")
        boolean skipSiteLinks();

        @Option(helpRequest = true)
        boolean help();
    }

    public static void main(String args[]) {
        Options options;
        Cli<Options> cli = CliFactory.createCli(Options.class);

        try {
            options = cli.parseArguments(args);
        } catch (HelpRequestedException e) {
            log.info(cli.getHelpMessage());
            return;
        } catch (ArgumentValidationException e) {
            log.error("Invalid argument", e);
            log.error(cli.getHelpMessage());
            return;
        }
        if (options.verbose()) {
            log.info("Verbose mode activated");
            // Assumes logback which is pretty safe in main.
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(getResource("logback-verbose.xml"));
            } catch (JoranException je) {
                // StatusPrinter will handle this
            }
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
        }
        WikibaseRepository wikibaseRepository = new WikibaseRepository(options.wikidataSchema(), options.wikidataHost());
        URI sparqlUri;
        try {
            sparqlUri = new URI(options.sparqlUrl());
        } catch (URISyntaxException e) {
            log.error("Invalid url:  " + options.sparqlUrl() + " caused by " + e.getMessage());
            return;
        }
        Entity entityUris = new Entity(options.wikidataHost());
        EntityData entityDataUris = new EntityData(options.wikidataHost());
        RdfRepository rdfRepository = new RdfRepository(sparqlUri, entityUris);
        Change.Source<? extends Change.Batch> changeSource = buildChangeSource(options, rdfRepository,
                wikibaseRepository);
        if (changeSource == null) {
            return;
        }
        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("update %s");
        ExecutorService executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory.build());

        Munger munger = new Munger(entityDataUris, entityUris);
        if (options.skipSiteLinks()) {
            munger = munger.removeSiteLinks();
        }
        if (options.labelLanguages() != null) {
            munger = munger.limitLabelLanguages(options.labelLanguages());
        }
        if (options.singleLabelLanguages() != null) {
            munger = munger.singleLabelMode(options.singleLabelLanguages());
        }

        new Update<>(changeSource, wikibaseRepository, rdfRepository, munger, executor).run();
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
            if (ids.length != 2) {
                log.error("Invalid format for --ids.  Need <start>-<stop>.");
                return null;
            }
            return IdChangeSource.forItems(Long.parseLong(ids[0]), Long.parseLong(ids[1]), 30);
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
            log.info("Checking last update time");
            Date lastUpdate = rdfRepository.fetchLastUpdate();
            long minStartTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
            if (lastUpdate == null) {
                startTime = minStartTime;
                log.info("Defaulting start time to 30 days ago:  {}", inputDateFormat().format(new Date(startTime)));
            } else {
                if (lastUpdate.getTime() < minStartTime) {
                    log.error("RDF store reports the last update time is before the minimum safe poll time.  "
                            + "You will have to reload from scratch or you might have missing data.");
                    return null;
                }
                /*
                 * -2 seconds to because our precision is only 1 second and
                 * because it should be cheap to recheck that we have the right
                 * revision.
                 */
                // TODO finding a time to start from the RDF store like this
                // is dangerous because we always update using the most recent
                // version of the document so this will cause skipping.
                startTime = lastUpdate.getTime() - SECONDS.toMillis(2);
                log.info("Found start time in the RDF store: {}", inputDateFormat().format(new Date(startTime)));
            }
        }
        return new RecentChangesPoller(wikibaseRepository, new Date(startTime));
    }

    private final Meter updateMeter = new Meter();
    private final Meter batchAdvanced = new Meter();

    private Change.Source<B> changeSource;
    private final WikibaseRepository wikibase;
    private final RdfRepository rdfRepository;
    private final Munger munger;
    private final ExecutorService executor;

    public Update(Change.Source<B> changeSource, WikibaseRepository wikibase, RdfRepository rdfRepository,
            Munger munger, ExecutorService executor) {
        this.changeSource = changeSource;
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.munger = munger;
        this.executor = executor;
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

                // TODO wrap all retry-able exceptions in a special exception

                batchAdvanced.mark(batch.advanced());
                log.info("Polled up to {} at {} updates per second and {} {} per second", batch.upTo(),
                        meterReport(updateMeter), meterReport(batchAdvanced), batch.advancedUnits());
                if (batch.last()) {
                    break;
                }
                batch = changeSource.nextBatch(batch);
            } catch (RetryableException e) {
                log.warn("Error occurred during poll loop. Retrying.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.error("Syncing encountered a fatal exception", e);
                break;
            }
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
     */
    private void handleChange(Change change) throws RetryableException, ContainedException {
        log.debug("Received revision information {}", change);
        if (change.revision() >= 0 && rdfRepository.hasRevision(change.entityId(), change.revision())) {
            log.debug("RDF repostiroy already has this revision, skipping.");
            return;
        }
        rdfRepository.sync(change.entityId(),
                munger.munge(change.entityId(), wikibase.fetchRdfForEntity(change.entityId())));
        updateMeter.mark();
    }

    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }
}
