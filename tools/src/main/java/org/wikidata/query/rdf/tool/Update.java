package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.IdListChangeSource;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller;
import org.wikidata.query.rdf.tool.options.UpdateOptions;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Update tool.
 */
public final class Update {
    private static final Logger log = LoggerFactory.getLogger(Update.class);

    private Update() {
        // this class should never be instantiated
    }

    /**
     * Run updates configured from the command line.
     * @throws Exception on error
     */
    // Catching exception is OK in a main exception handler, more so since the
    // exception is rethrown
    @SuppressWarnings("checkstyle:IllegalCatch")
    public static void main(String[] args) throws Exception {
        RdfRepository rdfRepository = null;
        Updater<? extends Change.Batch> updater;

        try {
            UpdateOptions options = handleOptions(UpdateOptions.class, args);
            WikibaseRepository wikibaseRepository = buildWikibaseRepository(options);
            URI sparqlUri = sparqlUri(options);
            WikibaseUris uris = new WikibaseUris(options.wikibaseHost());
            rdfRepository = new RdfRepository(sparqlUri, uris);
            Change.Source<? extends Change.Batch> changeSource = buildChangeSource(options, rdfRepository,
                    wikibaseRepository);
            updater = createUpdater(options, wikibaseRepository, uris, rdfRepository, changeSource);
        } catch (Exception e) {
            log.error("Error during initialization.", e);
            if (rdfRepository != null) {
                rdfRepository.close();
            }
            throw e;
        }
        try (RdfRepository r = rdfRepository) {
            updater.run();
        } catch (Exception e) {
            log.error("Error during updater run.", e);
            throw e;
        }
    }

    /**
     * Create an @{link Updater}.
     *
     * @param options
     * @param wikibaseRepository
     * @param uris
     * @param rdfRepository
     * @param changeSource
     * @return a newly created updater
     */
    private static Updater<? extends Change.Batch> createUpdater(
            UpdateOptions options,
            WikibaseRepository wikibaseRepository,
            WikibaseUris uris,
            RdfRepository rdfRepository,
            Change.Source<? extends Change.Batch> changeSource) {
        int threads = options.threadCount();
        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("update %s");
        ExecutorService executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), threadFactory.build());

        Munger munger = mungerFromOptions(options);
        return new Updater<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                options.pollDelay(), uris, options.verify());
    }

    /**
     * Create the sparql URI from the given configuration.
     *
     * @param options
     * @return a newly created sparql URI
     */
    private static URI sparqlUri(UpdateOptions options) {
        URI sparqlUri;
        try {
            sparqlUri = new URI(options.sparqlUrl());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url:  " + options.sparqlUrl(), e);
        }
        return sparqlUri;
    }

    /**
     * Build a change source.
     *
     * @return null if non can be built - its ok to just exit - errors have been
     *         logged to the user
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private static Change.Source<? extends Change.Batch> buildChangeSource(UpdateOptions options, RdfRepository rdfRepository,
                                                                           WikibaseRepository wikibaseRepository) {
        if (options.idrange() != null) {
            String[] ids = options.idrange().split("-");
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
                throw new IllegalArgumentException("Invalid format for --idrange.  Need <start>-<stop>.");
            }
            return IdRangeChangeSource.forItems(start, end, options.batchSize());
        }
        if (options.ids() != null) {
            List<String> parsedIds = new ArrayList<String>(); // FIXME use OptionsUtils.splitByComma(options.ids())
            for (String idOpt: options.ids()) {
                if (idOpt.contains(",")) {
                    // Id list
                    parsedIds.addAll(Arrays.asList(idOpt.split(",")));
                    continue;
                }
                parsedIds.add(idOpt);
            }
            return new IdListChangeSource(parsedIds.toArray(new String[parsedIds.size()]), options.batchSize());
        }
        long startTime;
        if (options.start() != null) {
            try {
                startTime = outputDateFormat().parse(options.start()).getTime();
            } catch (java.text.ParseException e) {
                try {
                    startTime = inputDateFormat().parse(options.start()).getTime();
                } catch (java.text.ParseException e2) {
                    throw  new IllegalArgumentException("Invalid date: " + options.start(), e2);
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
                    throw new IllegalStateException("RDF store reports the last update time is before the minimum safe poll time.  "
                            + "You will have to reload from scratch or you might have missing data.");
                }
                startTime = leftOff.getTime();
                log.info("Found start time in the RDF store: {}", inputDateFormat().format(leftOff));
            }
        }
        return new RecentChangesPoller(wikibaseRepository, new Date(startTime), options.batchSize(), options.tailPollerOffset());
    }

    /**
     * Build WikibaseRepository object.
     *
     * @return null if non can be built - its ok to just exit - errors have been
     *         logged to the user
     */
    private static WikibaseRepository buildWikibaseRepository(UpdateOptions options) {
        if (options.entityNamespaces() == null) {
            return new WikibaseRepository(options.wikibaseScheme(), options.wikibaseHost());
        }

        String[] strEntityNamespaces = options.entityNamespaces().split(","); // FIXME use OptionsUtils.splitByComma(options.entityNamespaces())
        long[] longEntityNamespaces = new long[strEntityNamespaces.length];
        try {
            for (int i = 0; i < strEntityNamespaces.length; i++) {
                longEntityNamespaces[i] = Long.parseLong(strEntityNamespaces[i]);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for --entityNamespaces. Namespace index should be an integer.", e);
        }
        return new WikibaseRepository(options.wikibaseScheme(), options.wikibaseHost(), 0, longEntityNamespaces);
    }
}
