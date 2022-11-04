package org.wikidata.query.rdf.tool;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikimedia.utils.http.CustomRoutePlanner;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.httpclient.InstrumentedHttpClientConnectionManager;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;

/**
 * Utilities for dealing with HttpClient.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public final class HttpClientUtils {
    /**
     * Default timeout for http connections.
     */
    public static final Duration TIMEOUT = Duration.ofMillis(5000);

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientUtils.class);

    /** Configuration name for proxy host. */
    public static final String HTTP_PROXY_PROPERTY = "http.proxyHost";

    /** Configuration name for proxy port. */
    public static final String HTTP_PROXY_PORT_PROPERTY = "http.proxyPort";

    /** How many times we retry a failed HTTP call. */
    public static final int MAX_RETRIES = 6;
    /**
     * How many retries allowed on error.
     */
    private static final int RETRIES = 3;
    /**
     * Retry interval, in ms.
     */
    private static final int RETRY_INTERVAL = 500;


    /**
     * How long to delay after failing first HTTP call, in milliseconds.
     * Next retries would be slower exponentially by 2x until MAX_RETRIES is exhausted.
     * Note that the first retry is 2x HTTP_RETRY_DELAY due to the way Retryer is implemented.
     */
    public static final int HTTP_RETRY_DELAY = 1000;

    /**
     * Default HTTP User-Agent used by the system.
     */
    public static final String WDQS_DEFAULT_UA = "Wikidata Query Service Updater Bot";

    /**
     * Max number of connection pooled per route.
     */
    public static final int MAX_POOLED_CON_PER_ROUTE = 100;

    /**
     * Max number of connection pooled in total (per http client created).
     */
    public static final int MAX_POOLED_CON_PER_CLIENT = 100;

    private HttpClientUtils() {
        // Uncallable utility constructor
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
                                LOG.info("HTTP request failed: {}, attempt {}, will {}",
                                        attempt.getExceptionCause(),
                                        attempt.getAttemptNumber(),
                                        attempt.getAttemptNumber() < MAX_RETRIES ? "retry" : "fail");
                            }
                        }
                    })
                    .build();
    }

    public static CloseableHttpClient createHttpClient(HttpClientConnectionManager connectionManager,
                                                       String proxy, String proxyMapString,
                                                       int requestTimeout,
                                                       String userAgent) {

        return configureHttpClient(HttpClients.custom(), connectionManager, proxy, proxyMapString,
                requestTimeout, userAgent).build();
    }

    public static HttpClientBuilder configureHttpClient(HttpClientBuilder httpClientBuilder,
                                                        HttpClientConnectionManager connectionManager,
                                                        String proxy, String proxyMapString,
                                                        int requestTimeout,
                                                        String userAgent) {

        if (proxy != null && proxyMapString != null) {
            throw new IllegalArgumentException("Cannot set both proxy and proxy map property");
        }

        httpClientBuilder.setConnectionManager(connectionManager)
                .setRetryHandler(getRetryHandler(RETRIES))
                .setServiceUnavailableRetryStrategy(getRetryStrategy(RETRIES, RETRY_INTERVAL))
                .disableCookieManagement()
                .setUserAgent(userAgent)
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setSocketTimeout(requestTimeout)
                        .setConnectTimeout(requestTimeout)
                        .setConnectionRequestTimeout(requestTimeout)
                        .build());

        if (proxyMapString != null) {
            Map<String, HttpHost> proxyMap = createProxyMap(proxyMapString);
            httpClientBuilder.setRoutePlanner(new CustomRoutePlanner(proxyMap,
                    new DefaultRoutePlanner(DefaultSchemePortResolver.INSTANCE)));
        }

        if (proxy != null) {
            HttpHost httpHost = HttpHost.create(proxy);
            httpClientBuilder.setProxy(httpHost);
        }
        return httpClientBuilder;
    }

    public static CloseableHttpClient createHttpClient(HttpClientConnectionManager connectionManager, String proxy, String proxyMapString, int requestTimeout) {
        return createHttpClient(connectionManager, proxy, proxyMapString, requestTimeout, getUserAgent());
    }

    private static Map<String, HttpHost> createProxyMap(String proxyMapString) {
        return CustomRoutePlanner.createMapFromString(proxyMapString);
    }

    private static String getUserAgent() {
        return System.getProperty("http.userAgent", WDQS_DEFAULT_UA);
    }

    public static HttpClientConnectionManager createConnectionManager(MetricRegistry registry, int soTimeout) {
        InstrumentedHttpClientConnectionManager connectionManager = new InstrumentedHttpClientConnectionManager(registry);
        configureConnectionManager(soTimeout, connectionManager);
        return connectionManager;
    }

    public static HttpClientConnectionManager createPooledConnectionManager(int soTimeout) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(-1L, SECONDS);
        configureConnectionManager(soTimeout, connectionManager);
        return connectionManager;
    }

    private static void configureConnectionManager(int soTimeout, PoolingHttpClientConnectionManager connectionManager) {
        connectionManager.setDefaultMaxPerRoute(MAX_POOLED_CON_PER_ROUTE);
        connectionManager.setMaxTotal(MAX_POOLED_CON_PER_CLIENT);
        IdleConnectionEvictor connectionEvictor = new IdleConnectionEvictor(connectionManager, 1L, SECONDS);
        connectionEvictor.start();
        // workaround issue https://issues.apache.org/jira/browse/HTTPCLIENT-1478 when using a proxy (not fixed in 4.4)
        connectionManager.setDefaultSocketConfig(SocketConfig.copy(SocketConfig.DEFAULT).setSoTimeout(soTimeout).build());
    }

    /**
     * Create retry handler.
     * Note: this is for retrying I/O exceptions.
     * @param max Maximum retries number.
     */
    private static HttpRequestRetryHandler getRetryHandler(final int max) {
        return (exception, executionCount, context) -> {
            LOG.debug("Exception in attempt {}", executionCount, exception);
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
     * Return retry strategy for "service unavailable".
     * This one handles 503 and 429 by retrying it after a fixed period.
     * TODO: 429 may contain header that we may want to use for retrying?
     * @param max Maximum number of retries.
     * @param interval Interval between retries, ms.
     * @see org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy
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
}
