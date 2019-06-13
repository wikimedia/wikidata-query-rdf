package org.wikidata.query.rdf.blazegraph.filters;

import static org.assertj.core.api.Assertions.assertThat;

import javax.servlet.Filter;
import javax.servlet.FilterChain;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

@RunWith(MockitoJUnitRunner.class)
public class RealAgentFilterUnitTest {
    private Filter filter = new RealAgentFilter();

    @Mock private FilterChain chain;

    @Test
    public void emptyUserAgent() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(403);
    }

    @Test
    public void badUserAgent() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader("User-Agent", "-");
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(403);
    }

    @Test
    public void goodUserAgent() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();
        request.addHeader("User-Agent", "Ceci n'est pas un user agent");
        filter.doFilter(request, response, chain);
        assertThat(response.getStatus()).isEqualTo(200);
    }
}
