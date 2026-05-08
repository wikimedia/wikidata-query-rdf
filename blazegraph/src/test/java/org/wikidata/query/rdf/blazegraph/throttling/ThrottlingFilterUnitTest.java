package org.wikidata.query.rdf.blazegraph.throttling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Instant;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

@RunWith(MockitoJUnitRunner.class)
public class ThrottlingFilterUnitTest {

    @Mock private FilterChain chain;

    @Test
    public void banMessageContainsBanEndDate() {
        Instant until = Instant.parse("2007-12-03T10:15:30.00Z");
        String message = ThrottlingFilter.formattedBanMessage(until);

        assertThat(
                message,
                containsString("until 2007-12-03T10:15:30Z, "));
    }

    @Test
    public void localhostIPv4RequestPassesThroughWithoutThrottling() throws ServletException, IOException {
        ThrottlingFilter filter = createFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");
        request.addParameter("throttleMe", "true");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    public void localhostIPv6RequestPassesThroughWithoutThrottling() throws ServletException, IOException {
        ThrottlingFilter filter = createFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("0:0:0:0:0:0:0:1");
        request.addParameter("throttleMe", "true");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    public void nonLocalhostRequestIsSubjectToThrottling() throws ServletException, IOException {
        ThrottlingFilter filter = createFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("1.2.3.4");
        request.addParameter("throttleMe", "true");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain, never()).doFilter(request, response);
    }

    private ThrottlingFilter createFilter() throws ServletException {
        ThrottlingFilter filter = new ThrottlingFilter();
        filter.init(new MockFilterConfig());
        return filter;
    }
}
