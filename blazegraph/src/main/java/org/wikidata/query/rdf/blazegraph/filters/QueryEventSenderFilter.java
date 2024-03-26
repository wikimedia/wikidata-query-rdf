package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.WikibaseContextListener;
import org.wikidata.query.rdf.blazegraph.events.AsyncEventSender;
import org.wikidata.query.rdf.blazegraph.events.EventFileSender;
import org.wikidata.query.rdf.blazegraph.events.EventHttpSender;
import org.wikidata.query.rdf.blazegraph.events.EventSender;
import org.wikidata.query.rdf.blazegraph.events.QueryEvent;
import org.wikidata.query.rdf.blazegraph.events.QueryEventGenerator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This filter assumes that it is configured to only track
 * read queries.
 */
public class QueryEventSenderFilter extends MonitoredFilter implements QueryEventSenderMXBean {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private EventSender sender;
    private QueryEventGenerator queryEventGenerator;
    private String enableIfHeader;
    private String disableIfHeader;
    private final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    private final AtomicInteger queryCounter = new AtomicInteger(0);
    private final AtomicLong startedQueries = new AtomicLong(0);

    public QueryEventSenderFilter() {
    }

    @VisibleForTesting
    QueryEventSenderFilter(EventSender sender, QueryEventGenerator eventGenerator, String enableIfHeader, String disableIfHeader) {
        this.sender = sender;
        this.queryEventGenerator = eventGenerator;
        this.enableIfHeader = enableIfHeader;
        this.disableIfHeader = disableIfHeader;
    }

    @Override
    @SuppressFBWarnings(value = "MDM_INETADDRESS_GETLOCALHOST", justification = "are there alternatives?")
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        FilterConfiguration config = new FilterConfiguration(filterConfig, FilterConfiguration.WDQS_CONFIG_PREFIX);
        String wdqsHostname;
        try {
            wdqsHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new ServletException(e);
        }
        String streamName = config.loadStringParam("event-gate-sparql-query-stream", "blazegraph.sparql-query");

        this.queryEventGenerator = new QueryEventGenerator(() -> UUID.randomUUID().toString(),
                Clock.systemUTC(),
                wdqsHostname,
                streamName,
                operatingSystemMXBean::getSystemLoadAverage);
        this.enableIfHeader = config.loadStringParam("enable-event-sender-if-header");
        this.disableIfHeader = config.loadStringParam("disable-event-sender-if-header");
        boolean eventFileSenderEnabled = config.loadBooleanParam("file-event-sender", false);
        int maxCap = config.loadIntParam("queue-size", 1000);
        int maxEventsPerHttpRequest = config.loadIntParam("http-max-events-per-request", 10);
        EventSender eventSender = eventFileSenderEnabled ? createFileEventSender(config) : createHttpEventSender(config);
        this.sender = AsyncEventSender.wrap(maxCap, maxEventsPerHttpRequest, eventSender);
    }

    private EventSender createFileEventSender(FilterConfiguration config) {
        Path path = Paths.get(config.loadStringParam("file-event-sender-filepath"));
        return new EventFileSender(path);
    }

    private EventSender createHttpEventSender(FilterConfiguration config) {
        String httpEndPoint = config.loadStringParam("event-gate-endpoint");
        int httpReadTimeout = config.loadIntParam("http-read-timeout", EventHttpSender.DEFAULT_READ_TIMEOUT);
        int httpConTimeout = config.loadIntParam("http-con-timeout", EventHttpSender.DEFAULT_CON_TIMEOUT);
        if (httpEndPoint == null) {
            return e -> true; // /dev/null
        }
        return EventHttpSender.build(httpEndPoint, httpReadTimeout, httpConTimeout);
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        if (!canLogQueryEvent(httpRequest)) {
            filterChain.doFilter(httpRequest, httpResponse);
            return;
        }

        Instant start = queryEventGenerator.instant();
        boolean succeed = false;
        int countBefore = queryCounter.getAndIncrement();
        startedQueries.incrementAndGet();
        try {
            filterChain.doFilter(servletRequest, servletResponse);
            succeed = true;
        } finally {
            int countAfter = queryCounter.decrementAndGet();
            int responseStatus;
            Instant end = queryEventGenerator.instant();
            if (succeed) {
                responseStatus = httpResponse.getStatus();
            } else {
                responseStatus = 500;
            }
            String defaultNamespace = (String) servletRequest.getServletContext().getAttribute(WikibaseContextListener.BLAZEGRAPH_DEFAULT_NAMESPACE);
            QueryEvent event = queryEventGenerator.generateQueryEvent(httpRequest, responseStatus, Duration.between(start, end),
                    start, defaultNamespace, countBefore, countAfter);
            if (!sender.push(event)) {
                log.warn("Cannot sent event for {} (queue full?)", event.getMetadata().getStream());
            }
        }
    }

    private boolean canLogQueryEvent(HttpServletRequest httpRequest) {
        if (!Strings.isNullOrEmpty(enableIfHeader) && httpRequest.getHeader(enableIfHeader) == null) {
            return false;
        }
        if (!Strings.isNullOrEmpty(disableIfHeader) && httpRequest.getHeader(disableIfHeader) != null) {
            return false;
        }
        if (!queryEventGenerator.hasValidPath(httpRequest)) {
            return false;
        }
        return httpRequest.getParameter(QueryEventGenerator.QUERY_PARAM) != null;
    }

    @Override
    public void destroy() {
        try {
            sender.close();
        } catch (IOException e) {
            log.error("Exception thrown while closing event sender.", e);
        }
    }

    @Override
    public int getRunningQueries() {
        return queryCounter.get();
    }

    @Override
    public long getStartedQueries() {
        return startedQueries.get();
    }
}
