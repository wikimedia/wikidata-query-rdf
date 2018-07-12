package org.wikidata.query.rdf.tool;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static java.lang.Integer.parseInt;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.INPUT_DATE_FORMATTER;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.OUTPUT_DATE_FORMATTER;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.IdListChangeSource;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.change.KafkaPoller;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.options.OptionsUtils;
import org.wikidata.query.rdf.tool.options.UpdateOptions;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Update tool.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity") // object initialization and wiring needs to be cleaned up
public final class Update {
    /** Request timeout property. */
    public static final String TIMEOUT_PROPERTY = RdfRepository.class + ".timeout";
    /** Configuration name for proxy host. */
    public static final String HTTP_PROXY_PROPERTY = "http.proxyHost";
    /** Configuration name for proxy port. */
    public static final String HTTP_PROXY_PORT_PROPERTY = "http.proxyPort";
    /** How many times we retry a failed HTTP call. */
    public static final int MAX_RETRIES = 6;
    /**
     * How long to delay after failing first HTTP call, in milliseconds.
     * Next retries would be slower exponentially by 2x until MAX_RETRIES is exhausted.
     * Note that the first retry is 2x HTTP_RETRY_DELAY due to the way Retryer is implemented.
     */
    public static final int HTTP_RETRY_DELAY = 1000;

    private static final Logger log = LoggerFactory.getLogger(Update.class);

    private static final String MAX_DAYS_BACK_NAME = "wikibaseMaxDaysBack";

    private Update() {
        // this class should never be instantiated
    }

    static {
        // Set negative cache to 5s, should be enough to catch immediate fails
        // but not too long to make one-time failure stick.
        Security.setProperty("networkaddress.cache.negative.ttl", "5");
    }

    /**
     * Run updates configured from the command line.
     * @throws Exception on error
     */
    // Catching exception is OK in a main exception handler, more so since the
    // exception is rethrown
    @SuppressWarnings("checkstyle:IllegalCatch")
    public static void main(String[] args) throws Exception {
        RdfRepository rdfRepository;
        Updater<? extends Change.Batch> updater = null;
        WikibaseRepository wikibaseRepository;
        HttpClient httpClient = null;

        try {
            UpdateOptions options = handleOptions(UpdateOptions.class, args);
            wikibaseRepository = buildWikibaseRepository(options);
            URI sparqlUri = sparqlUri(options);
            WikibaseUris uris = OptionsUtils.makeWikibaseUris(options);
            httpClient = buildHttpClient(getHttpProxyHost(), getHttpProxyPort());
            rdfRepository = buildRdfRepository(sparqlUri, uris, httpClient);
            Change.Source<? extends Change.Batch> changeSource = buildChangeSource(
                    options, rdfRepository, wikibaseRepository);
            updater = createUpdater(options, wikibaseRepository, uris, rdfRepository, changeSource);
        } catch (Exception e) {
            log.error("Error during initialization.", e);
            if (httpClient != null) {
                httpClient.stop();
            }
            if (updater != null) {
                updater.close();
            }
            throw e;
        }
        try (
                WikibaseRepository w = wikibaseRepository;
                Updater<? extends Change.Batch> u = updater;
        ) {
            updater.run();
        } catch (Exception e) {
            log.error("Error during updater run.", e);
            throw e;
        } finally {
            httpClient.stop();
        }
    }

    @Nullable
    public static String getHttpProxyHost() {
        return System.getProperty(HTTP_PROXY_PROPERTY);
    }

    @Nullable
    public static Integer getHttpProxyPort() {
        String p = System.getProperty(HTTP_PROXY_PORT_PROPERTY);
        if (p == null) return null;
        return Integer.valueOf(p);
    }

    private static RdfRepository buildRdfRepository(
            URI sparqlUri, WikibaseUris uris,
            HttpClient httpClient) {
        return new RdfRepository(uris, buildRdfClient(sparqlUri, httpClient));
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
        if (options.testMode()) {
            return new TestUpdater<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                    options.pollDelay(), uris, options.verify());
        }
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
     * @return the change source
     * @throws IllegalArgumentException if the options are invalid
     * @throws IllegalStateException if the left of date is too ancient
     */
    @Nonnull
    private static Change.Source<? extends Change.Batch> buildChangeSource(UpdateOptions options, RdfRepository rdfRepository,
        WikibaseRepository wikibaseRepository) {
        if (options.idrange() != null) {
            return buildIdRangeChangeSource(options.idrange(), options.batchSize());
        }
        if (options.ids() != null) {
            return buildIdListChangeSource(options.ids(), options.batchSize());
        }
        if (options.kafkaBroker() != null) {
            // If we have explicit start time, we ignore kafka offsets
            boolean ignoreStoredOffset = (options.start() != null || options.resetKafka());
            return KafkaPoller.buildKafkaPoller(options.kafkaBroker(), options.consumerId(),
                    OptionsUtils.splitByComma(options.clusters()), wikibaseRepository.getUris(),
                    options.batchSize(),
                    getStartTime(options.start(), rdfRepository, options.init()),
                    rdfRepository, ignoreStoredOffset);
        }
        return buildRecentChangePollerChangeSource(
                rdfRepository, wikibaseRepository,
                options.start(), options.init(), options.batchSize(), options.tailPollerOffset());
    }

    /** Builds a change source polling latest changes from wikidata. */
    @Nonnull
    private static Change.Source<? extends Change.Batch> buildRecentChangePollerChangeSource(
            RdfRepository rdfRepository, WikibaseRepository wikibaseRepository,
            String start, boolean init, int batchSize, int tailPollerOffset) {
        return new RecentChangesPoller(wikibaseRepository,
                getStartTime(start, rdfRepository, init),
                batchSize, tailPollerOffset);
    }

    private static Instant getStartTime(String start, RdfRepository rdfRepository, boolean init) {
        Instant startTime;
        if (start != null) {
            startTime = parseDate(start);
            if (init) {
                // Initialize left off time to start time
                rdfRepository.updateLeftOffTime(startTime);
            }
        } else {
            log.info("Checking where we left off");
            Instant leftOff = rdfRepository.fetchLeftOffTime();
            Integer maxDays = Integer.valueOf(System.getProperty(MAX_DAYS_BACK_NAME, "30"));
            Instant minStartTime = Instant.now().minus(maxDays.intValue(), ChronoUnit.DAYS);
            if (leftOff == null) {
                startTime = minStartTime;
                log.info("Defaulting start time to {} days ago: {}", maxDays, startTime);
            } else {
                if (leftOff.isBefore(minStartTime)) {
                    throw new IllegalStateException("RDF store reports the last update time is before the minimum safe poll time.  "
                            + "You will have to reload from scratch or you might have missing data.");
                }
                startTime = leftOff;
                log.info("Found start time in the RDF store: {}", leftOff);
            }
        }
        return startTime;
    }

    /**
     * Parse a string to a date, trying output format or the input format.
     *
     * @throws IllegalArgumentException if the date cannot be parsed with either format.
     */
    private static Instant parseDate(String dateStr) {
        try {
            return OUTPUT_DATE_FORMATTER.parse(dateStr, Instant::from);
        } catch (DateTimeParseException e) {
            try {
                return INPUT_DATE_FORMATTER.parse(dateStr, Instant::from);
            } catch (DateTimeParseException e2) {
                throw  new IllegalArgumentException("Invalid date: " + dateStr, e2);
            }
        }
    }

    /** Builds a change source based on a list of IDs. */
    private static Change.Source<? extends Change.Batch> buildIdListChangeSource(List<String> ids, int batchSize) {
        List<String> parsedIds = new ArrayList<>(); // FIXME use OptionsUtils.splitByComma(options.ids())
        for (String idOpt: ids) {
            if (idOpt.contains(",")) {
                // Id list
                parsedIds.addAll(Arrays.asList(idOpt.split(",")));
                continue;
            }
            parsedIds.add(idOpt);
        }
        return new IdListChangeSource(parsedIds.toArray(new String[parsedIds.size()]), batchSize);
    }

    /** Builds a change source based on a range of IDs. */
    private static Change.Source<? extends Change.Batch> buildIdRangeChangeSource(String idrange, int batchSize) {
        String[] ids = idrange.split("-");
        long start;
        long end;
        switch (ids.length) {
        case 1:
            if (!Character.isDigit(ids[0].charAt(0))) {
                // Not a digit - probably just single ID
                return new IdListChangeSource(ids, batchSize);
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
        return IdRangeChangeSource.forItems(start, end, batchSize);
    }

    /**
     * Produce base Wikibase URL from options.
     * @param options
     */
    private static URI getWikibaseUrl(UpdateOptions options) {
        if (options.wikibaseUrl() != null) {
            try {
                return new URI(options.wikibaseUrl());
            } catch (URISyntaxException e) {
                throw new FatalException("Unable to build Wikibase url", e);
            }
        }
        URIBuilder baseUrl = new URIBuilder();
        baseUrl.setHost(options.wikibaseHost());
        baseUrl.setScheme(options.wikibaseScheme());
        try {
            return baseUrl.build();
        } catch (URISyntaxException e) {
            throw new FatalException("Unable to build Wikibase url", e);
        }
    }

    /**
     * Build WikibaseRepository object.
     *
     * @return null if non can be built - its ok to just exit - errors have been
     *         logged to the user
     */
    private static WikibaseRepository buildWikibaseRepository(UpdateOptions options) {
        WikibaseRepository repo;
        if (options.entityNamespaces() == null) {
            repo = new WikibaseRepository(getWikibaseUrl(options));
        } else {
            long[] longEntityNamespaces = OptionsUtils.splitByComma(Arrays.asList(options.entityNamespaces())).stream().
                mapToLong(option -> Long.parseLong(option)).toArray();

            repo = new WikibaseRepository(getWikibaseUrl(options), longEntityNamespaces);
        }
        repo.setCollectConstraints(options.constraints());
        return repo;
    }

    public static RdfClient buildRdfClient(URI uri, HttpClient httpClient) {
        int timeout = parseInt(System.getProperty(TIMEOUT_PROPERTY, "-1"));
        Duration timeoutDuration = Duration.of(timeout, SECONDS);
        return new RdfClient(httpClient, uri, buildHttpClientRetryer(), timeoutDuration);
    }

    @SuppressWarnings("checkstyle:IllegalCatch") // Exception is part of Jetty's HttpClient contract
    public static HttpClient buildHttpClient(@Nullable String httpProxyHost, @Nullable Integer httpProxyPort) {
        HttpClient httpClient = new HttpClient(new SslContextFactory(true/* trustAll */));
        if (httpProxyHost != null && httpProxyPort != null) {
            final ProxyConfiguration proxyConfig = httpClient.getProxyConfiguration();
            final HttpProxy proxy = new HttpProxy(httpProxyHost, httpProxyPort);
            proxy.getExcludedAddresses().add("localhost");
            proxy.getExcludedAddresses().add("127.0.0.1");
            proxyConfig.getProxies().add(proxy);
        }
        try {
            httpClient.start();
        // Who would think declaring it as throws Exception is a good idea?
        } catch (Exception e) {
            throw new RuntimeException("Unable to start HttpClient", e);
        }
        return httpClient;
    }

    public static Retryer<ContentResponse> buildHttpClientRetryer() {
        return RetryerBuilder.<ContentResponse>newBuilder()
                    .retryIfExceptionOfType(TimeoutException.class)
                    .retryIfExceptionOfType(ExecutionException.class)
                    .retryIfExceptionOfType(IOException.class)
                    .retryIfRuntimeException()
                    .withWaitStrategy(exponentialWait(HTTP_RETRY_DELAY, 10, TimeUnit.SECONDS))
                    .withStopStrategy(stopAfterAttempt(MAX_RETRIES))
                    .withRetryListener(new RetryListener() {
                        @Override
                        public <V> void onRetry(Attempt<V> attempt) {
                            if (attempt.hasException()) {
                                log.info("HTTP request failed: {}, attempt {}, will {}",
                                        attempt.getExceptionCause(),
                                        attempt.getAttemptNumber(),
                                        attempt.getAttemptNumber() < MAX_RETRIES ? "retry" : "fail");
                            }
                        }
                    })
                    .build();
    }
}
