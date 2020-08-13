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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.FilterChain;
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
        String headerFlag = "headerFlag";
        String defaultNS = "defaultNS";

        when(eventGenerator.instant())
                .thenReturn(Instant.EPOCH)
                .thenReturn(Instant.EPOCH.plus(1, SECONDS));
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(eq(WikibaseContextListener.BLAZEGRAPH_DEFAULT_NAMESPACE))).thenReturn(defaultNS);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getHeader(eq(headerFlag))).thenReturn("yes");
        when(request.getServletContext()).thenReturn(context);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        when(eventGenerator.generateQueryEvent(eq(request), eq(200), eq(Duration.ofSeconds(1)), eq(Instant.EPOCH), eq(defaultNS)))
                .thenReturn(generatedEvent);
        FilterChain filterChain = mock(FilterChain.class);
        when(response.getStatus()).thenReturn(200);
        QueryEventSenderFilter filter = new QueryEventSenderFilter(receivedEvents::add, eventGenerator, headerFlag);
        filter.doFilter(request, response, filterChain);
        assertThat(receivedEvents).containsExactly(generatedEvent);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
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

        when(eventGenerator.generateQueryEvent(eq(request), eq(500), eq(Duration.ofSeconds(1)), eq(Instant.EPOCH), eq(defaultNS)))
                .thenReturn(generatedEvent);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        FilterChain filterChain = mock(FilterChain.class);
        doThrow(new ServletException("failure")).when(filterChain).doFilter(eq(request), eq(response));
        QueryEventSenderFilter filter = new QueryEventSenderFilter(receivedEvents::add, eventGenerator, null);
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
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, null);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString());
    }

    @Test
    public void testIgnoreOnBadRequestURI() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(false);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, null);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString());
    }

    @Test
    public void testIgnoreOnMissingHeaderFlag() throws IOException, ServletException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        String headerFlag = "headerFlag";

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter(eq(QueryEventGenerator.QUERY_PARAM))).thenReturn(SPARQL_QUERY);
        when(request.getHeader(eq(headerFlag))).thenReturn(null);
        when(eventGenerator.hasValidPath(eq(request))).thenReturn(true);
        HttpServletResponse response = mock(HttpServletResponse.class);

        FilterChain filterChain = mock(FilterChain.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter((e) -> true, eventGenerator, headerFlag);
        filter.doFilter(request, response, filterChain);
        verify(filterChain, times(1)).doFilter(eq(request), eq(response));
        verify(eventGenerator, never()).instant();
        verify(eventGenerator, never()).generateQueryEvent(any(), anyInt(), any(), any(), anyString());
    }

    @Test
    public void testDestroy() throws IOException {
        QueryEventGenerator eventGenerator = mock(QueryEventGenerator.class);
        EventSender sender = mock(EventSender.class);
        QueryEventSenderFilter filter = new QueryEventSenderFilter(sender, eventGenerator, null);
        filter.destroy();
        verify(sender, times(1)).close();
    }
}
