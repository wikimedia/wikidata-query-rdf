package org.wikidata.query.rdf.tool;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;

/**
 * Utilities for dealing with HttpClient.
 */
public final class HttpClientUtils {
    private static final Logger log = LoggerFactory.getLogger(HttpClientUtils.class);

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

    private HttpClientUtils() {
        // Uncallable utility constructor
    }

    /**
     * Configure request to ignore cookies.
     */
    public static void ignoreCookies(HttpRequestBase request) {
        RequestConfig noCookiesConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build();
        request.setConfig(noCookiesConfig);
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
