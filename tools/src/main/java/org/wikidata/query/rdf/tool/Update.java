package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
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
public class Update implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Update.class);

    public interface Options {
        @Option(shortName = "w", defaultValue = "www.wikidata.org", description = "Wikidata host")
        String wikidataHost();

        @Option(defaultValue = "https", description = "Wikidata url schema")
        String wikidataSchema();

        @Option(shortName = "v")
        boolean verbose();

        @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
        String start();

        @Option(shortName = "u", description = "URL to post updates and queries.")
        String sparqlUrl();

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
        long startTime;
        if (options.start() != null) {
            try {
                startTime = outputDateFormat().parse(options.start()).getTime();
            } catch (java.text.ParseException e) {
                try {
                    startTime = inputDateFormat().parse(options.start()).getTime();
                } catch (java.text.ParseException e2) {
                    log.error("Invalid date:  {}", options.start());
                    return;
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
                    return;
                }
                /*
                 * -2 seconds to because our precision is only 1 second and
                 * because it should be cheap to recheck that we have the right
                 * revision.
                 */
                startTime = lastUpdate.getTime() - SECONDS.toMillis(2);
                log.info("Found start time in the RDF store: {}", inputDateFormat().format(new Date(startTime)));
            }
        }
        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("update %s");
        ExecutorService executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory.build());

        new Update(wikibaseRepository, rdfRepository, entityUris, entityDataUris, executor, startTime).run();
    }

    private final Meter updateMeter = new Meter();
    private final Meter startTimeMeter = new Meter();

    private final WikibaseRepository wikibase;
    private final RdfRepository rdfRepository;
    private final Entity entityUris;
    private final EntityData entityDataUris;
    private final ExecutorService executor;

    private long nextStartTime;
    private JSONObject nextContinue;
    private long currentBatchStartTime;
    private JSONObject currentBatchContinue;

    public Update(WikibaseRepository wikibase, RdfRepository rdfRepository, Entity entityUris,
            EntityData entityDataUris, ExecutorService executor, long startTime) {
        this.wikibase = wikibase;
        this.rdfRepository = rdfRepository;
        this.entityUris = entityUris;
        this.entityDataUris = entityDataUris;
        nextStartTime = startTime;
        this.executor = executor;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<Future<?>> tasks = new ArrayList<>();
                for (Object rco : pollRecentChanges()) {
                    final JSONObject rc = (JSONObject) rco;
                    tasks.add(executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    handleChange(rc);
                                    return;
                                } catch (RetryableException e) {
                                    log.warn("Retryable error syncing.  Retrying.", e);
                                } catch (ContainedException e) {
                                    log.warn("Contained error syncing.  Giving up on " + rc.get("title"), e);
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
                successfullyCompletedBatch();
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
    private void handleChange(JSONObject rc) throws RetryableException, ContainedException {
        log.debug("Received revision information {}", rc);
        String entityId = rc.get("title").toString();
        long revision = (long) rc.get("revid");
        long namespace = (long) rc.get("ns");
        if (namespace != 0) {
            // We only index the main namespace.
            return;
        }
        // TODO deletes
        // TODO it'd be reasonably simple to fork/join here
        if (rdfRepository.hasRevision(entityId, revision)) {
            log.debug("RDF repostiroy already has this revision, skipping.");
            return;
        }
        Munger munger = new Munger(entityDataUris, entityUris);
        rdfRepository.sync(entityId, munger.munge(wikibase.fetchRdfForEntity(entityId)));
        updateMeter.mark();
    }

    /**
     * Poll recent changes and update lastContinue and nextStartTime so that
     * subsequent calls to this method will get the next most recent changes.
     */
    private JSONArray pollRecentChanges() throws RetryableException {
        JSONObject recentChanges = wikibase.fetchRecentChanges(nextStartTime, nextContinue);
        currentBatchContinue = (JSONObject) recentChanges.get("continue");
        JSONArray result = (JSONArray) ((JSONObject) recentChanges.get("query")).get("recentchanges");
        DateFormat df = inputDateFormat();
        for (Object rco : result) {
            JSONObject rc = (JSONObject) rco;
            try {
                currentBatchStartTime = Math.max(nextStartTime, df.parse(rc.get("timestamp").toString()).getTime());
            } catch (java.text.ParseException e) {
                throw new RetryableException("Parse error from api", e);
            }
        }
        return result;
    }

    private void successfullyCompletedBatch() {
        startTimeMeter.mark(currentBatchStartTime - nextStartTime);
        nextStartTime = currentBatchStartTime;
        nextContinue = currentBatchContinue;
        // Show the user the polled time - one seconds because we can't
        // be sure we got the whole second
        log.info("Polled up to {} at {} updates per second and {} seconds per second",
                inputDateFormat().format(new Date(nextStartTime - 1000)), meterReport(updateMeter, 1),
                meterReport(startTimeMeter, 1000));
    }

    private String meterReport(Meter meter, float divisor) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate() / divisor,
                meter.getFiveMinuteRate() / divisor, meter.getFifteenMinuteRate() / divisor);
    }
}
