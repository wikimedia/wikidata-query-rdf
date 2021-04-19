package org.wikidata.query.rdf.tool.wikibase;

import static com.google.common.collect.ImmutableSet.copyOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.wikidata.query.rdf.tool.MapperUtils.getObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PushbackInputStream;
import java.io.Serializable;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.protocol.HttpContext;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.RDFParserSupplier;
import org.wikidata.query.rdf.tool.utils.StreamDumper;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.httpclient.InstrumentedHttpClientConnectionManager;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wraps Wikibase api.
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class WikibaseRepository implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(WikibaseRepository.class);

    /**
     * Timeout for communications to Wikidata, in ms.
     */
    private static final String TIMEOUT_MILLIS = "5000";
    /**
     * Request timeout property.
     */
    private static final String TIMEOUT_PROPERTY = WikibaseRepository.class.getName() + ".timeout";
    /**
     * Request proxy property.
     */
    private static final String PROXY_PROPERTY = WikibaseRepository.class.getName() + ".proxy";
    /**
     * How many retries allowed on error.
     */
    private static final int RETRIES = 3;

    /**
     * Retry interval, in ms.
     */
    private static final int RETRY_INTERVAL = 500;

    /**
     * Standard representation of dates in Mediawiki API (ISO 8601).
     */
    public static final String INPUT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * DateTimeFormatter object that parses from and formats to the date
     * in the format that wikibase returns, i.e. yyyy-MM-dd'T'HH:mm:ss
     */
    public static final DateTimeFormatter INPUT_DATE_FORMATTER = new DateTimeFormatterBuilder()
                .appendPattern(INPUT_DATE_FORMAT)
                .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
                .toFormatter()
                .withZone(ZoneId.of("Z"));

    /**
     * DateTimeFormatter object that parses from and formats to the date
     * in the format that wikibase wants as input, i.e. yyyyMMddHHmmss.
     */
    public static final DateTimeFormatter OUTPUT_DATE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyyMMddHHmmss")
            .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
            .toFormatter()
            .withZone(ZoneId.of("Z"));

    /**
     * Configured proxy for requests.
     */
    private static final String PROXY_HOST;
    private static final int PROXY_PORT;

    static {
        String proxy = System.getProperty(PROXY_PROPERTY);
        if (proxy != null) {
            String[] split = proxy.split(":");
            PROXY_HOST = split[0];
            PROXY_PORT = Integer.parseInt(split[1]);
        } else {
            PROXY_HOST = null;
            PROXY_PORT = -1;
        }
    }

    /**
     * HTTP client for wikibase.
     */
    private final CloseableHttpClient client;

    /**
     * Connection manager for Wikibase.
     */
    private final HttpClientConnectionManager connectionManager;

    /**
     * Configured timeout for requests.
     */
    private final int requestTimeout = Integer
            .parseInt(System.getProperty(TIMEOUT_PROPERTY, TIMEOUT_MILLIS));

    /**
     * Request configuration including timeout.
     */
    private final RequestConfig configWithTimeout = RequestConfig.custom()
            .setSocketTimeout(requestTimeout)
            .setConnectTimeout(requestTimeout)
            .setConnectionRequestTimeout(requestTimeout).build();
    /**
     * Builds uris to get stuff from wikibase.
     */
    private final Uris uris;

    /**
     * Should we also collect constraints?
     */
    private boolean collectConstraints;

    /**
     * How old (hours) revision should be to switch to lastest revision fetch.
     */
    private Duration revisionCutoff;

    /**
     * Object mapper used to deserialize JSON messages from Wikidata.
     *
     * Note that this mapper is configured to ignore unknown properties.
     */
    private final ObjectMapper mapper = getObjectMapper();

    /**
     * Collects the time spent collecting RDF statements from Wikibase.
     */
    private final Timer rdfFetchTimer;
    /**
     * Collects the time spent collecting entity data from Wikibase.
     */
    private final Timer entityFetchTimer;
    /**
     * Collects the time spent collecting constraints data from Wikibase.
     */
    private final Timer constraintFetchTimer;

    /**
     * Used to dump HTTP responses to file.
     */
    private final StreamDumper streamDumper;

    private final RDFParserSupplier rdfParserSupplier;

    public WikibaseRepository(Uris uris, boolean collectConstraints, MetricRegistry metricRegistry, StreamDumper streamDumper,
                              @Nullable Duration revisionCutoff, RDFParserSupplier rdfParserSupplier) {
        this.uris = uris;
        this.collectConstraints = collectConstraints;
        this.rdfFetchTimer = metricRegistry.timer("rdf-fetch-timer");
        this.entityFetchTimer = metricRegistry.timer("entity-fetch-timer");
        this.constraintFetchTimer = metricRegistry.timer("constraint-fetch-timer");
        connectionManager = createConnectionManager(metricRegistry, requestTimeout);
        client = createHttpClient(connectionManager);
        this.streamDumper = streamDumper;
        this.revisionCutoff = revisionCutoff;
        this.rdfParserSupplier = rdfParserSupplier;
    }

    /**
     * Return retry strategy for "service unavailable".
     * This one handles 503 and 429 by retrying it after a fixed period.
     * TODO: 429 may contain header that we may want to use for retrying?
     * @param max Maximum number of retries.
     * @param interval Interval between retries, ms.
     * @see DefaultServiceUnavailableRetryStrategy
     */
    private static ServiceUnavailableRetryStrategy getRetryStrategy(final int max, final int interval) {
        // This is the same as DefaultServiceUnavailableRetryStrategy but also handles 429
        return new ServiceUnavailableRetryStrategy() {
            @Override
            public boolean retryRequest(final HttpResponse response, final int executionCount, final HttpContext context) {
                return executionCount <= max &&
                    (response.getStatusLine().getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE ||
                    response.getStatusLine().getStatusCode() == 429);
            }

            @Override
            public long getRetryInterval() {
                return interval;
            }
        };
    }

    /**
     * Create retry handler.
     * Note: this is for retrying I/O exceptions.
     * @param max Maximum retries number.
     */
    private static HttpRequestRetryHandler getRetryHandler(final int max) {
        return (exception, executionCount, context) -> {
            log.debug("Exception in attempt {}", executionCount, exception);
            if (executionCount >= max) {
                // Do not retry if over max retry count
                return false;
            }
            if (exception instanceof InterruptedIOException) {
                // Timeout - also includes ConnectTimeoutException
                return true;
            }
            if (exception instanceof UnknownHostException) {
                // Unknown host
                return false;
            }
            if (exception instanceof SSLException) {
                // SSL handshake exception
                return false;
            }

            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // Retry if the request is considered idempotent
            return !(request instanceof HttpEntityEnclosingRequest);
        };
    }

    /**
     * Fetch recent changes starting from nextStartTime or continuing from
     * lastContinue depending on the contents of lastContinue way to use
     * MediaWiki. See RecentChangesPoller for how to poll these. Or just use it.
     *
     * @param nextStartTime if lastContinue is null then this is the start time
     *            of the query
     * @param batchSize the number of recent changes to fetch
     * @return result of query
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public RecentChangeResponse fetchRecentChangesByTime(Instant nextStartTime, int batchSize) throws RetryableException {
        return fetchRecentChanges(nextStartTime, null, batchSize);
    }

    /**
     * Fetch recent changes starting from nextStartTime or continuing from
     * lastContinue depending on the contents of lastContinue way to use
     * MediaWiki. See RecentChangesPoller for how to poll these. Or just use it.
     *
     * @param nextStartTime if lastContinue is null then this is the start time
     *            of the query
     * @param batchSize the number of recent changes to fetch
     * @param lastContinue Continuation object from last batch, or null.
     * @return result of query
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public RecentChangeResponse fetchRecentChanges(Instant nextStartTime, Continue lastContinue, int batchSize)
            throws RetryableException {
        URI uri = uris.recentChanges(nextStartTime, lastContinue, batchSize);
        log.debug("Polling for changes from {}", uri);
        HttpGet request = new HttpGet(uri);
        request.setConfig(configWithTimeout);
        try {
            return checkApi(getJson(request, RecentChangeResponse.class));
        } catch (UnknownHostException | SocketException e) {
            // We want to bail on this, since it happens to be sticky for some reason
            throw new RuntimeException(e);
        } catch (JsonParseException | JsonMappingException  e) {
            // An invalid response will probably not fix itself with a retry, so let's bail
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RetryableException("Error fetching recent changes", e);
        }
    }

    /**
     * Get input stream for entity, if it exists and not empty.
     * We need this because our proxy can convert 204 responses
     * to 200 responses with empty body.
     */
    private InputStream getInputStream(HttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            PushbackInputStream in = new PushbackInputStream(entity.getContent());
            int firstByte = in.read();
            if (firstByte != -1) {
                in.unread(firstByte);
                return in;
            }
        }
        return null;
    }

    /**
     * Collect TTL statements from single URL.
     * @throws RetryableException if there's a retryable error
     */
    private void collectStatementsFromUrl(URI uri, StatementCollector collector, Timer timer) throws RetryableException {
        RDFParser parser = this.rdfParserSupplier.get(collector);
        HttpGet request = new HttpGet(uri);
        request.setConfig(configWithTimeout);
        log.debug("Fetching rdf from {}", uri);
        try (Timer.Context timerContext = timer.time()) {
            try (CloseableHttpResponse response = client.execute(request)) {
                if (response.getStatusLine().getStatusCode() == 404) {
                    throw new WikibaseEntityFetchException(uri, WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND);
                }
                if (response.getStatusLine().getStatusCode() == 204) {
                    throw new WikibaseEntityFetchException(uri, WikibaseEntityFetchException.Type.NO_CONTENT);
                }
                if (response.getStatusLine().getStatusCode() >= 300) {
                    throw new WikibaseEntityFetchException(uri, WikibaseEntityFetchException.Type.UNEXPECTED_RESPONSE);
                }
                try (InputStream in = streamDumper.wrap(getInputStream(response))) {
                    if (in == null) {
                        throw new WikibaseEntityFetchException(uri, WikibaseEntityFetchException.Type.EMPTY_RESPONSE);
                    }
                    parser.parse(new InputStreamReader(in, UTF_8), uri.toString());
                }
            }
        } catch (UnknownHostException | SocketException | SSLHandshakeException e) {
            // We want to bail on this, since it happens to be sticky for some reason
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RetryableException("Error fetching RDF for " + uri, e);
        } catch (RDFParseException | RDFHandlerException e) {
            throw new ContainedException("RDF parsing error for " + uri, e);
        }
    }

    private boolean isChangeRecent(Change change) {
        if (revisionCutoff == null || revisionCutoff.isZero()) {
            // if revision cutoff is not set, we still use latest fetch
            return false;
        }
        return change.timestamp() != null &&
                change.timestamp().isAfter(Instant.now().minus(revisionCutoff));
    }

    /**
     * Fetch the RDF for change in the entity.
     *
     * If the change is recent (set by revisionCutoff duration) then we fetch specific
     * revision using revision=1234 in the URL. This allows us to use varnish cache.
     * If the change is old, there's serious chance this revision is not the latest for
     * the item, thus we're using cache-busting nocache=... URL to fetch the latest one.
     * TODO: it might be better to use some syntax that allows fetching "this revision or later"
     * to still benefit from some caching, or have some caching on the back end, but this
     * is for further improvements.
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public Collection<Statement> fetchRdfForEntity(Change entityChange) throws RetryableException {
        if (entityChange.revision() > 0 && isChangeRecent(entityChange)) {
            return fetchRdfForEntity(entityChange.entityId(), entityChange.revision());
        }
        // Don't use chronology header for non-recent events since it's pointless - database should
        // have caught up long before that.
        return fetchRdfForEntity(entityChange.entityId(), -1);
    }

    /**
     * Fetch the RDF for some entity.
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    @VisibleForTesting
    public Collection<Statement> fetchRdfForEntity(String entityId) throws RetryableException {
        return fetchRdfForEntity(entityId, -1);
    }

    /**
     * Fetch the RDF for some entity.
     * If revision is good (above 0) it will fetch by revision.
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    @SuppressWarnings("resource") // stop() and close() are the same
    public Collection<Statement> fetchRdfForEntity(String entityId, long revision) throws RetryableException {
        Timer.Context timerContext = rdfFetchTimer.time();
        StatementCollector collector = new StatementCollector();
        collectStatementsFromUrl(uris.rdf(entityId, revision), collector, entityFetchTimer);
        if (collectConstraints) {
            try {
                // TODO: constraints should probably handled by its own update pipeline
                //  and possibly be stored in a dedicated graph
                // Re-using the same error detection patterns seems suspicious
                collectStatementsFromUrl(uris.constraints(entityId), collector, constraintFetchTimer);
            } catch (ContainedException ex) {
                // TODO: add RetryableException here?
                // Skip loading constraints on fail, it's not the reason to give up
                // on the whole item.
                log.info("Failed to load constraints: {}", ex.getMessage());
            }
        }
        log.debug("Done in {} ms", timerContext.stop() / 1000_000);
        return collector.getStatements();
    }


    /**
     * Perform an HTTP request and return the JSON in the response body.
     *
     * @param request request to perform
     * @return json response
     * @throws IOException if there is an error parsing the json or if one is
     *             thrown receiving the data
     */
    private <T extends WikibaseResponse> T getJson(HttpRequestBase request, Class<T> valueType)
            throws IOException {
        try (CloseableHttpResponse response = client.execute(request)) {
            return mapper.readValue(response.getEntity().getContent(), valueType);
        }
    }

    /**
     * Check that a response from the Mediawiki api isn't an error.
     *
     * @param response the response
     * @return the response again
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    private <T extends WikibaseResponse> T checkApi(T response) throws RetryableException {
        Object error = response.getError();
        if (error != null) {
            throw new RetryableException("Error result from Mediawiki:  " + error);
        }
        return response;
    }

    /**
     * Check that a namespace is valid wikibase entity namespace.
     *
     * @param namespace the namespace index
     */
    public boolean isEntityNamespace(long namespace) {
        return uris.isEntityNamespace(namespace);
    }

    /**
     * Check if the entity ID is a valid entity ID.
     */
    public boolean isValidEntity(String name) {
        return name.matches("^[A-Za-z0-9:]+$");
    }

    @Override
    public void close() throws IOException {
        try {
            client.close();
        } finally {
            connectionManager.shutdown();
        }
    }

    private static CloseableHttpClient createHttpClient(HttpClientConnectionManager connectionManager) {
        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setRetryHandler(getRetryHandler(RETRIES))
                .setServiceUnavailableRetryStrategy(getRetryStrategy(RETRIES, RETRY_INTERVAL))
                .disableCookieManagement()
                .setUserAgent(getUserAgent());

        return (PROXY_HOST != null ? httpClientBuilder.setProxy(new HttpHost(PROXY_HOST, PROXY_PORT)) : httpClientBuilder)
                .build();
    }

    private static String getUserAgent() {
        return System.getProperty("http.userAgent", "Wikidata Query Service Updater Bot");
    }

    private static InstrumentedHttpClientConnectionManager createConnectionManager(MetricRegistry registry, int soTimeout) {
        InstrumentedHttpClientConnectionManager connectionManager = new InstrumentedHttpClientConnectionManager(registry);
        connectionManager.setDefaultMaxPerRoute(100);
        connectionManager.setMaxTotal(100);
        IdleConnectionEvictor connectionEvictor = new IdleConnectionEvictor(connectionManager, 1L, SECONDS);
        connectionEvictor.start();
        // workaround issue https://issues.apache.org/jira/browse/HTTPCLIENT-1478 when using a proxy (not fixed in 4.4)
        connectionManager.setDefaultSocketConfig(SocketConfig.copy(SocketConfig.DEFAULT).setSoTimeout(soTimeout).build());
        return connectionManager;
    }

    /**
     * URIs used for accessing wikibase.
     */
    public static class Uris implements Serializable {
        /**
         * Path which should be used to retrieve Entity data.
         */
        public static final String DEFAULT_ENTITY_DATA_PATH = "/wiki/Special:EntityData/";
        /**
         * URL of the API endpoint.
         */
        public static final String DEFAULT_API_PATH = "/w/api.php";
        public static final Set<Long> DEFAULT_ENTITY_NAMESPACES = ImmutableSet.of(0L, 120L);
        /**
         * Item and Property namespaces.
         */
        private final Set<Long> entityNamespaces;
        /**
         * Base URL for Wikibase.
         */
        private final URI baseUrl;

        private final String apiBasePath;
        private final String entityDataPath;

        public Uris(URI baseUrl, Set<Long> entityNamespaces, String apiBasePath, String entityDataPath) {
            this.baseUrl = baseUrl;
            this.entityNamespaces = copyOf(entityNamespaces);
            this.apiBasePath = apiBasePath;
            this.entityDataPath = entityDataPath;
        }

        public static Uris withWikidataDefaults(URI baseUrl) {
            return new Uris(baseUrl, DEFAULT_ENTITY_NAMESPACES, DEFAULT_API_PATH, DEFAULT_ENTITY_DATA_PATH);
        }

        public static Uris withWikidataDefaults(String url) {
            try {
                return withWikidataDefaults(new URI(url));
            } catch (URISyntaxException e) {
                throw new FatalException("Bad URL: " + url, e);
            }
        }

        /**
         * Uri to get the recent changes.
         *
         * @param startTime the first date to poll from - usually if
         *            continueObject isn't null this is ignored by wikibase
         * @param continueObject Continue object from the last request
         * @param batchSize maximum number of results we want back from wikibase
         */
        public URI recentChanges(Instant startTime, Continue continueObject, int batchSize) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "query");
            builder.addParameter("list", "recentchanges");
            builder.addParameter("rcdir", "newer");
            builder.addParameter("rcprop", "title|ids|timestamp");
            builder.addParameter("rcnamespace", getEntityNamespacesString("|"));
            builder.addParameter("rclimit", Integer.toString(batchSize));
            if (continueObject == null) {
                builder.addParameter("continue", "");
                builder.addParameter("rcstart", startTime.toString());
            } else {
                builder.addParameter("continue", continueObject.getContinue());
                builder.addParameter("rccontinue", continueObject.getRcContinue());
            }
            return build(builder);
        }

        private URIBuilder entityURIBuilder(String entityId) {
            URIBuilder builder = builder();
            /*
             * Note that we could use /entity/%s.ttl for production Wikidata but
             * not all Wikibase instances have the rewrite rule set up. I'm
             * looking at you test.wikidata.org
             */
            builder.setPath(baseUrl.getPath() + entityDataPath + entityId + ".ttl");
            builder.addParameter("flavor", "dump");
            return builder;
        }

        /**
         * Uri to get the rdf for an entity and revision.
         * Checks whether revision is good for fetching.
         */
        public URI rdf(String entityId, long revision) {
            if (revision > 0) {
                return rdfRevision(entityId, revision);
            }
            return rdf(entityId);
        }

        /**
         * Uri to get the rdf for an entity.
         */
        public URI rdf(String entityId) {
            URIBuilder builder = entityURIBuilder(entityId);
            // Cache is not our friend, try to work around it
            builder.addParameter("nocache", String.valueOf(Instant.now().toEpochMilli()));
            return build(builder);
        }

        /**
         * Uri to get the rdf for an entity and revision.
         * This assumes revision is OK (i.e. positive).
         */
        public URI rdfRevision(String entityId, long revision) {
            URIBuilder builder = entityURIBuilder(entityId);
            builder.addParameter("revision", Long.toString(revision));
            return build(builder);
        }

        private URIBuilder constraintsURIBuilder(String entityId) {
            URIBuilder builder = builder();
            builder.setPath(baseUrl.getPath() + "/wiki/" + entityId);
            builder.addParameter("action", "constraintsrdf");
            return builder;
        }

        /**
         * Uri to get the rdf for constraints status of an entity.
         */
        public URI constraints(String entityId) {
            URIBuilder builder = constraintsURIBuilder(entityId);
            // Cache is not our friend, try to work around it
            builder.addParameter("nocache", String.valueOf(Instant.now().toEpochMilli()));
            return build(builder);
        }

        /**
         * Build a URIBuilder for wikibase apis.
         */
        private URIBuilder apiBuilder() {
            URIBuilder builder = builder();
            builder.setPath(baseUrl.getPath() + apiBasePath);
            builder.addParameter("format", "json");
            return builder;
        }

        /**
         * Build a URIBuilder for wikibase requests.
         */
        public URIBuilder builder() {
            return new URIBuilder(baseUrl);
        }

        /**
         * Build a URI from an URI builder, throwing a FatalException if it
         * fails.
         */
        private URI build(URIBuilder builder) {
            try {
                return builder.build();
            } catch (URISyntaxException e) {
                throw new FatalException("Unable to build url!?", e);
            }
        }

        /**
         * The wikibase host.
         */
        public String getHost() {
            return baseUrl.getHost();
        }

        /**
         * Check that a namespace is valid wikibase entity namespace.
         *
         * @param namespace the namespace index
         */
        public boolean isEntityNamespace(long namespace) {
            return entityNamespaces.contains(namespace);
        }

        /**
         * The wikibase entity namespace indexes joined with a delimiter.
         */
        private String getEntityNamespacesString(String delimiter) {
            return entityNamespaces.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(delimiter));
        }

    }

    /**
     * Extract timestamp from continue JSON object.
     * @return Timestamp as date
     * @throws DateTimeParseException When data is in the wrong format
     * @throws NumberFormatException When data is in the wrong format
     */
    @SuppressFBWarnings(value = "STT_STRING_PARSING_A_FIELD", justification = "low priority to fix")
    public Change getChangeFromContinue(Continue nextContinue) {
        if (nextContinue == null) {
            return null;
        }
        final String[] parts = nextContinue.getRcContinue().split("\\|");
        return new Change("DUMMY", -1, OUTPUT_DATE_FORMATTER.parse(parts[0], Instant::from), Long.parseLong(parts[1]));
    }

    /**
     * Get repository URI setup.
     */
    public Uris getUris() {
        return uris;
    }

    /**
     * Should we collect constraints?
     */
    @VisibleForTesting
    public void setCollectConstraints(boolean collectConstraints) {
        this.collectConstraints = collectConstraints;
    }

    /**
     * How old shout revision be to use latest fetch?
     */
    @VisibleForTesting
    public void setRevisionCutoff(Duration cutoff) {
        this.revisionCutoff = cutoff;
    }

    /**
     * Notifies repository that a batch of changes has been processed.
     * Used for dumper maintenance for now.
     */
    public void batchDone() {
        streamDumper.rotate();
    }
}
