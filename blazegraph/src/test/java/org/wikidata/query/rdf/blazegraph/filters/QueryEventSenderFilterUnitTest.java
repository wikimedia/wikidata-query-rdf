package org.wikidata.query.rdf.blazegraph.filters;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.wikidata.query.rdf.blazegraph.WikibaseContextListener;
import org.wikidata.query.rdf.blazegraph.events.Event;
import org.wikidata.query.rdf.blazegraph.events.EventSender;
import org.wikidata.query.rdf.blazegraph.events.QueryEvent;
import org.wikidata.query.rdf.blazegraph.events.QueryEventGenerator;

public class QueryEventSenderFilterUnitTest {
    private static final String SPARQL_QUERY = "SELECT ?item ?itemLabel WHERE { " +
            "?item wdt:P31 wd:Q2934. " +
            "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". } }";

    @Test
    public void testQueryEvent() throws IOException, ServletException {
        List<Event> receivedEvents = new ArrayList<>();
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        QueryEvent generatedEvent = mock(QueryEvent.class);
        String enableIfHeaderFlag = "enableHeaderFlag";
        String disableIfHeaderFlag = "disableHeaderFlag";
        String defaultNS = "defaultNS";

        when(eventGenerator.instant())
                .thenReturn(Instant.EPOCH)
                .thenReturn(Instant.EPOCH.plus(1, SECONDS));
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(eq(WikibaseContextListener.BLAZEGRAPH_DEFAULT_NAMESPACE))).thenReturn(defaultNS);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getHeader(eq(enableIfHeaderFlag))).thenReturn("yes");
        when(request.getHeader(eq(disableIfHeaderFlag))).thenReturn(null);
        when(request.getServletContext()).thenReturn(context);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(eventGenerator.generateQueryEvent(eq(request), eq(200), eq(Duration.ofSeconds(1)), eq(Instant.EPOCH), eq(defaultNS), eq(0), eq(0)))
                .thenReturn(generatedEvent);
        FilterChain filterChain = mock(FilterChain.class);
        when(response.getStatus()).thenReturn(200);
        QueryEventSenderFilter filter = new QueryEventSenderFilter(receivedEvents::add, eventGenerator, enableIfHeaderFlag, disableIfHeaderFlag);
        filter.doFilter(request, response, filterChain);
        assertThat(receivedEvents).containsExactly(generatedEvent);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        assertThat(filter.getStartedQueries()).isEqualTo(1L);
    }

    @Test
    public void testQueryEventIsGeneratedOnDownstreamFailure() throws IOException, ServletException {
        List<Event> receivedEvents = new ArrayList<>();
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        QueryEvent generatedEvent = mock(QueryEvent.class);
        String defaultNS = "defaultNS";

        when(eventGenerator.instant())
                .thenReturn(Instant.EPOCH)
                .thenReturn(Instant.EPOCH.plus(1, SECONDS));
        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(eq(WikibaseContextListener.BLAZEGRAPH_DEFAULT_NAMESPACE))).thenReturn(defaultNS);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getServletContext()).thenReturn(context);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(eventGenerator.generateQueryEvent(eq(request), eq(500), eq(Duration.ofSeconds(1)), eq(Instant.EPOCH), eq(defaultNS), eq(0), eq(0)))
                .thenReturn(generatedEvent);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        FilterChain filterChain = mock(FilterChain.class);
        doThrow(new ServletException("failure")).when(filterChain).doFilter(eq(request), eq(response));
        QueryEventSenderFilter filter = new QueryEventSenderFilter(receivedEvents::add, eventGenerator, null, null);
        Throwable servletException = catchThrowable(() -> filter.doFilter(request, response, filterChain));
        assertThat(servletException).isInstanceOf(ServletException.class);
        assertThat(receivedEvents).containsExactly(generatedEvent);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
    }

    @Test
    public void testIgnoreOnMissingQueryParam() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        String defaultNS = "defaultNS";

        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(eq(WikibaseContextListener.BLAZEGRAPH_DEFAULT_NAMESPACE))).thenReturn(defaultNS);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServletContext()).thenReturn(context);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, null, null);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString(), anyInt(), anyInt());
    }

    @Test
    public void testIgnoreOnBadRequestURI() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(false);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, null, null);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString(), eq(0), eq(0));
    }

    @Test
    public void testIgnoreOnMissingEnableIfHeaderFlag() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        String headerFlag = "headerFlag";

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getHeader(eq(headerFlag))).thenReturn(null);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, headerFlag, null);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString(), eq(0), eq(0));
    }

    @Test
    public void testIgnoreOnDisableIfHeaderFlag() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        String headerFlag = "headerFlag";

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getHeader(eq(headerFlag))).thenReturn("true");
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, null, headerFlag);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString(), eq(0), eq(0));
    }

    @Test
    public void testDestroy() throws IOException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        EventSender sender = mock(EventSender.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter(sender, eventGenerator, null, null);
        filter.destroy();
        verify(sender, times(1)).close();
    }

    @Test
    public void testMXBeanRegistered() throws Exception {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        EventSender sender = mock(EventSender.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter(sender, eventGenerator, null, null);
        FilterConfig config = mock(FilterConfig.class);
        when(config.getFilterName()).thenReturn("QueryEventSenderFilter");
        filter.init(config);
        ObjectName objectName = new ObjectName("org.wikidata.query.rdf.blazegraph.filters.QueryEventSenderFilter",
                "filterName", "QueryEventSenderFilter");
        MBeanInfo mBeanInfo = ManagementFactory.getPlatformMBeanServer().getMBeanInfo(objectName);
        assertThat(mBeanInfo.getAttributes()).hasSize(2);
        assertThat(Arrays.stream(mBeanInfo.getAttributes()).map(MBeanFeatureInfo::getName)).containsExactlyInAnyOrder("RunningQueries", "StartedQueries");
    }
}
