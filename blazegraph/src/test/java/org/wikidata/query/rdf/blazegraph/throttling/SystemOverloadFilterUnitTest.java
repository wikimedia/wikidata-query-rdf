package org.wikidata.query.rdf.blazegraph.throttling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.web.MockFilterConfig;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class SystemOverloadFilterUnitTest {

    @Mock private OperatingSystemMXBean operatingSystemMXBean;
    @Mock private HttpServletRequest request;
    @Mock private HttpServletResponse response;
    @Mock private FilterChain chain;

    private SystemOverloadFilter filter;

    @Before
    public void setupRequestHeaders() {
        when(request.getHeader("X-BIGDATA-READ-ONLY")).thenReturn("");
    }

    @After
    public void destroyFilter() {
        if (filter != null) filter.destroy();
    }

    @Test(expected = ServletException.class)
    public void lowLimitShouldBeLowerThanHighLimit() throws ServletException {
        filter = createFilter(12.0, 8.0);
    }

    @Test
    public void ratioOfRequestsToDrop() throws ServletException {
        filter = createFilter(8.0, 12.0);

        Map<Double, Double> loadToTest = ImmutableMap.<Double, Double>builder()
                .put(6.0, 0.0)
                .put(8.0, 0.0)
                .put(9.0, 0.25)
                .put(10.0, 0.5)
                .put(11.0, 0.75)
                .put(12.0, 1.0)
                .put(13.0, 1.0)
                .build();

        loadToTest.forEach((load, expectedRatio) -> {
            setLoadAvg(load);
            assertThat(filter.ratioOfRequestsToDrop()).isCloseTo(expectedRatio, offset(0.01));
        });
    }

    @Test
    public void percentOfRequestsDroppedIsRoughlyCorrect() throws ServletException, IOException {
        filter = createFilter(8.0, 12.0);
        setLoadAvg(9.0);

        for (int i = 0; i < 1000; i++) {
            filter.doFilter(request, response, chain);
        }

        // verify that the number of invocations is within some bounds, since
        // the number of requests actually dropped is based on a pseudo-RNG.
        // assuming perfect distribution 25% of requests should pass
        // allow between 15% and 35%
        verify(chain, atLeast(150)).doFilter(request, response);
        verify(chain, atMost(350)).doFilter(request, response);
    }

    @Test
    public void requestsWithoutEnableIfHeaderAreNeverThrottled() throws ServletException, IOException {
        filter = createFilter(8.0, 12.0);
        setLoadAvg(13.0);

        // remove the header configuration
        reset(request);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    public void uninitializedFilterPassThrough() throws ServletException, IOException {
        MockFilterConfig filterConfig = new MockFilterConfig();
        filter = new SystemOverloadFilter();
        filter.init(filterConfig);

        setLoadAvg(100.0);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    private SystemOverloadFilter createFilter(double low, double high) throws ServletException {
        MockFilterConfig filterConfig = new MockFilterConfig();
        filterConfig.addInitParameter("system-load-low-limit", Double.toString(low));
        filterConfig.addInitParameter("system-load-high-limit", Double.toString(high));
        filterConfig.addInitParameter("enable-if-header", "X-BIGDATA-READ-ONLY");
        SystemOverloadFilter filter = new SystemOverloadFilter();
        filter.init(filterConfig);
        filter.operatingSystemMXBean = operatingSystemMXBean;

        return filter;
    }

    private void setLoadAvg(double load) {
        when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(load);
    }

}
