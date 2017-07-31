package org.wikidata.query.rdf.blazegraph.filters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class LoggingFilterTest {

    private LoggingFilter filter = new LoggingFilter();
    @Mock private ServletResponse response;
    @Mock private FilterChain chain;

    @Test
    public void ipAndUserAgentAreAddedToMDC() throws IOException, ServletException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("1.2.3.4");
        request.addHeader("User-Agent", "UA1");

        doAnswer(invocation -> {
            assertThat(MDC.get("IPAddress"), equalTo("1.2.3.4"));
            assertThat(MDC.get("UserAgent"), equalTo("UA1"));
            return null;
        }).when(chain).doFilter(request, response);


        filter.doFilter(request, response, chain);
    }

    @Test
    public void nullIpAndUserAgentDontCauseIssues() throws IOException, ServletException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr(null);
        // dont set User-Agent header

        doAnswer(invocation -> {
            assertNull(MDC.get("IPAddress"));
            assertNull(MDC.get("UserAgent"));
            return null;
        }).when(chain).doFilter(request, response);


        filter.doFilter(request, response, chain);
    }

    @Test
    public void ipAndUserAgentAreCleanedUp() throws IOException, ServletException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("1.2.3.4");
        request.addHeader("User-Agent", "UA1");

        filter.doFilter(request, response, chain);

        assertNull(MDC.get("IPAddress"));
        assertNull(MDC.get("UserAgent"));
    }

    @Test
    public void nonHttpRequestsDontCauseIssues() throws IOException, ServletException {
        ServletRequest request = mock(ServletRequest.class);
        doAnswer(invocation -> {
            assertNull(MDC.get("UserAgent"));
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }

}
