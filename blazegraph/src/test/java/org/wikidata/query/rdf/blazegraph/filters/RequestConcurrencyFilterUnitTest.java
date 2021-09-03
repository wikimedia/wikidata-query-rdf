package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;

import javax.servlet.ServletException;

import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class RequestConcurrencyFilterUnitTest {

    @Test
    public void singleRequestIsPassedThrough() throws ServletException, IOException {
        RequestConcurrencyFilter filter = createFilter(10);

        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), chain);
    }

    private RequestConcurrencyFilter createFilter(int concurrency) throws ServletException {
        RequestConcurrencyFilter filter = new RequestConcurrencyFilter();
        MockFilterConfig filterConfig = new MockFilterConfig();
        filterConfig.addInitParameter("concurrency", Integer.toString(concurrency));
        filter.init(filterConfig);
        return filter;
    }

}
