package org.wikidata.query.rdf.tool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

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
    public static void main(String[] args) throws Exception {
        UpdateOptions options = handleOptions(UpdateOptions.class, args);
        WikibaseRepository wikibaseRepository = buildWikibaseRepository(options);
        if (wikibaseRepository == null) {
            return;
        }
        URI sparqlUri;
        try {
            sparqlUri = new URI(options.sparqlUrl());
        } catch (URISyntaxException e) {
            log.error("Invalid url:  " + options.sparqlUrl(), e);
            return;
        }
        WikibaseUris uris = new WikibaseUris(options.wikibaseHost());
        try (RdfRepository rdfRepository = new RdfRepository(sparqlUri, uris)) {
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
            new Updater<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                options.pollDelay(), uris, options.verify()).run();
        }
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
                log.error("Invalid format for --idrange.  Need <start>-<stop>.");
                return null;
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
            log.error("Invalid value for --entityNamespaces. Namespace index should be an integer.", e);
            return null;
        }
        return new WikibaseRepository(options.wikibaseScheme(), options.wikibaseHost(), 0, longEntityNamespaces);
    }
}
