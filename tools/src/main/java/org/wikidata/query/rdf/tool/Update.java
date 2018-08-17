package org.wikidata.query.rdf.tool;

import static java.lang.Integer.parseInt;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClient;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClientRetryer;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyHost;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyPort;
import static org.wikidata.query.rdf.tool.change.ChangeSourceContext.buildChangeSource;
import static org.wikidata.query.rdf.tool.change.ChangeSourceContext.getStartTime;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;
import static org.wikidata.query.rdf.tool.options.UpdateOptions.startInstant;

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.options.OptionsUtils.WikibaseOptions;
import org.wikidata.query.rdf.tool.options.UpdateOptions;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.github.rholder.retry.Retryer;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Update tool.
 */
@SuppressWarnings({
        "checkstyle:classfanoutcomplexity", // object initialization and wiring needs to be cleaned up
        "checkstyle:IllegalCatch" // Catching exception is OK in a main exception handler, more so since the exception is rethrown
})
public final class Update {
    /** Request timeout property. */
    public static final String TIMEOUT_PROPERTY = RdfRepository.class + ".timeout";

    private static final Logger log = LoggerFactory.getLogger(Update.class);

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
    public static void main(String[] args) throws Exception {
        Closer closer = Closer.create();
        try {
            Updater<? extends Change.Batch> updater = initialize(args, closer);
            run(updater);
        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
    }

    private static Updater<? extends Change.Batch> initialize(String[] args, Closer closer) throws URISyntaxException {
        try {
            UpdateOptions options = handleOptions(UpdateOptions.class, args);

            WikibaseRepository wikibaseRepository = new WikibaseRepository(UpdateOptions.uris(options), options.constraints());
            closer.register(wikibaseRepository);

            WikibaseUris wikibaseUris = WikibaseOptions.wikibaseUris(options);
            URI root = wikibaseRepository.getUris().builder().build();

            URI sparqlUri = UpdateOptions.sparqlUri(options);

            HttpClient httpClient = buildHttpClient(getHttpProxyHost(), getHttpProxyPort());
            closer.register(wrapHttpClient(httpClient));

            Retryer<ContentResponse> retryer = buildHttpClientRetryer();
            Duration rdfClientTimeout = getRdfClientTimeout();

            RdfClient rdfClient = new RdfClient(httpClient, sparqlUri, retryer, rdfClientTimeout);

            RdfRepository rdfRepository = new RdfRepository(wikibaseUris, rdfClient);

            Instant startTime = getStartTime(startInstant(options), rdfRepository, options.init());

            MetricRegistry metricRegistry = createMetricRegistry(closer);

            Change.Source<? extends Change.Batch> changeSource = buildChangeSource(
                    options, startTime, wikibaseRepository, rdfClient, root,
                    metricRegistry);

            Munger munger = mungerFromOptions(options);

            ExecutorService updaterExecutorService = createUpdaterExecutorService(options.threadCount());

            Updater<? extends Change.Batch> updater = createUpdater(
                    wikibaseRepository, wikibaseUris, rdfRepository, changeSource,
                    munger, updaterExecutorService,
                    options.pollDelay(), options.verify(), options.testMode(),
                    metricRegistry);
            closer.register(updater);
            return updater;
        } catch (Exception e) {
            log.error("Error during initialization.", e);
            throw e;
        }
    }

    private static void run(Updater<? extends Change.Batch> updater) {
        try {
            updater.run();
        } catch (Exception e) {
            log.error("Error during updater run.", e);
            throw e;
        }
    }

    /**
     * Create an @{link Updater}.
     */
    private static Updater<? extends Change.Batch> createUpdater(
            WikibaseRepository wikibaseRepository,
            WikibaseUris uris,
            RdfRepository rdfRepository,
            Change.Source<? extends Change.Batch> changeSource,
            Munger munger,
            ExecutorService executor,
            int pollDelay,
            boolean verify,
            boolean testMode,
            MetricRegistry metricRegistry) {

        if (testMode) {
            return new TestUpdater<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                    pollDelay, uris, verify, metricRegistry);
        }
        return new Updater<>(changeSource, wikibaseRepository, rdfRepository, munger, executor,
                pollDelay, uris, verify, metricRegistry);
    }

    private static ExecutorService createUpdaterExecutorService(int threadCount) {
        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("update %s");
        return new ThreadPoolExecutor(
                threadCount, threadCount,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory.build());
    }

    public static Duration getRdfClientTimeout() {
        int timeout = parseInt(System.getProperty(TIMEOUT_PROPERTY, "-1"));
        return Duration.of(timeout, ChronoUnit.SECONDS);
    }

    /**
     * Wrap HttpClient in a closeable.
     *
     * HttpClient does not implement Closeable. This provides a wrapper to
     * make its behaviour more coherent.
     */
    private static Closeable wrapHttpClient(HttpClient httpClient) {
        return () -> {
            try {
                httpClient.stop();
            } catch (Exception e) {
                throw new RuntimeException("Could not close HttpClient", e);
            }
        };
    }

    public static MetricRegistry createMetricRegistry(Closer closer) {
        MetricRegistry metrics = new MetricRegistry();
        JmxReporter reporter = closer.register(JmxReporter.forRegistry(metrics).build());
        reporter.start();
        return metrics;
    }
}
