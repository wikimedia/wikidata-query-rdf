package org.wikidata.query.rdf.blazegraph.filters;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Semaphore;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Limits the number of concurrent requests.
 *
 * Can be configured via the {@code concurrency} parameter.
 */
public class RequestConcurrencyFilter implements Filter {

    private Semaphore concurrency;
    private Duration retryAfterDuration;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        FilterConfiguration config = new FilterConfiguration(filterConfig, FilterConfiguration.WDQS_CONFIG_PREFIX);
        concurrency = new Semaphore(config.loadIntParam("concurrency", 50));
        retryAfterDuration = Duration.of(config.loadIntParam("retry-after-seconds", 120), SECONDS);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest && response instanceof HttpServletResponse)) {
            chain.doFilter(request, response);
        }

        if (!concurrency.tryAcquire()) {
            notifyUserMaxConcurrencyReached(
                    (HttpServletResponse) response,
                    retryAfterDuration);
        }

        try {
            chain.doFilter(request, response);
        } finally {
            concurrency.release();
        }
    }

    /**
     * Notify the user that we reached concurrency limit.
     *
     * @param response the response
     */
    private void notifyUserMaxConcurrencyReached(HttpServletResponse response, Duration duration) throws IOException {
        String retryAfter = Long.toString(duration.getSeconds());
        response.setHeader("Retry-After", retryAfter);
        response.sendError(429, format(ENGLISH, "Too Many Requests - Please retry in %s seconds.", retryAfter));
    }

    @Override
    public void destroy() {
        // nothing to cleanup
    }
}
