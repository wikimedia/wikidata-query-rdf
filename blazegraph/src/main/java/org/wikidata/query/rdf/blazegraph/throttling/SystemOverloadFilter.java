package org.wikidata.query.rdf.blazegraph.throttling;

import static org.wikidata.query.rdf.blazegraph.filters.FilterConfiguration.WDQS_CONFIG_PREFIX;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.filters.FilterConfiguration;
import org.wikidata.query.rdf.blazegraph.filters.MonitoredFilter;

import com.google.common.annotations.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A Servlet Filter that drops requests when the system load is high.
 *
 * A low and high limits can be configured ({@code system-load-low-limit} and
 * {@code system-load-high-limit}). When load is below the low limit, all
 * requests are processed. When the load is above the high limit, all requests
 * are dropped (with an HTTP 503 status). Between the limits, a linearly
 * increasing ratio of requests are dropped.
 *
 * If {@code enable-if-header} is configured, only request with that header
 * are affected, requests not containing that header are always processed.
 */
public class SystemOverloadFilter extends MonitoredFilter implements Filter, SystemOverloadFilterMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemOverloadFilter.class);

    @VisibleForTesting
    OperatingSystemMXBean operatingSystemMXBean;
    private double systemLoadLowLimit;
    private double systemLoadHighLimit;
    private String enableFilterIfHeader;

    private final LongAdder rejectedCount = new LongAdder();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        try {
            operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        } catch (IllegalArgumentException e) {
            operatingSystemMXBean = null;
            LOGGER.error("Could not load {}.", OperatingSystemMXBean.class.getSimpleName(), e);
        }

        FilterConfiguration config = new FilterConfiguration(filterConfig, WDQS_CONFIG_PREFIX);

        systemLoadLowLimit = config.loadDoubleParam("system-load-low-limit", -1.0);
        systemLoadHighLimit = config.loadDoubleParam("system-load-high-limit", -1.0);

        if (systemLoadLowLimit > systemLoadHighLimit) {
            throw new ServletException("system-load-low-limit should be lower than system-load-high-limit");
        }

        enableFilterIfHeader = config.loadStringParam("enable-if-header");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        // assume that this filter is only used in an HTTP context
        assert HttpServletRequest.class.isAssignableFrom(request.getClass());
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        assert HttpServletResponse.class.isAssignableFrom(response.getClass());
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (shouldBypassThrottling(httpRequest)) {
            chain.doFilter(request, response);
            return;
        }

        if (shouldDropRequest(ratioOfRequestsToDrop())) notifyOverloaded(httpResponse);
        else chain.doFilter(request, response);
    }

    @SuppressFBWarnings(value = "PREDICTABLE_RANDOM", justification = "We don't need a SecureRandom to drop a percentage of requests.")
    private boolean shouldDropRequest(double ratio) {
        return ThreadLocalRandom.current().nextDouble(1.0) > ratio;
    }

    @VisibleForTesting
    double ratioOfRequestsToDrop() {
        double systemLoad = operatingSystemMXBean.getSystemLoadAverage();

        if (systemLoad < systemLoadLowLimit) return 0.0;
        if (systemLoad > systemLoadHighLimit) return 1.0;

        return (systemLoad - systemLoadLowLimit) / (systemLoadHighLimit - systemLoadLowLimit);
    }

    /**
     * Check whether this request should have throttling enabled.
     *
     * @return true if throttling should be skipped
     */
    protected boolean shouldBypassThrottling(HttpServletRequest request) {
        // not initialized properly (should never happen)
        if (operatingSystemMXBean == null) return true;

        // load limits not configured, treat the filter as disabled
        if (systemLoadLowLimit <= 0.0 || systemLoadHighLimit <= 0.0) return true;

        // no "enableFilterIfHeader" configured, treat all requests
        if (enableFilterIfHeader == null) return false;

        // "enableFilterIfHeader" configured, only process requests that have that header
        return request.getHeader(enableFilterIfHeader) == null;
    }

    private void notifyOverloaded(HttpServletResponse response) throws IOException {
        response.sendError(503, "Service is overloaded, please try again later.");

        LOGGER.warn("Request throttled because of system load higher than {}.", systemLoadLowLimit);
        rejectedCount.increment();
    }

    @Override
    public long getRejectedCount() {
        return rejectedCount.longValue();
    }
}
