package org.wikidata.query.rdf.blazegraph.throttling;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.blazegraph.throttling.ThrottlingTestUtils.createRequest;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ThrottlerTest {

    private Cache<Object, Object> cache;

    @Before
    public void createCache() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build();
    }

    @Test
    public void requestAreThrottledOnlyWhenThrottlingHeaderIsPresent() {
        ThrottlerImpl throttler = new ThrottlerImpl(cache);

        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");
        request.addHeader("throttleMeHeader", "");
        assertThat(throttler.throttledUntil(new Object(), request), equalTo(Instant.EPOCH));

        request = createRequest("UA1", "1.2.3.4");
        assertThat(throttler.throttledUntil(new Object(), request), equalTo(Instant.MIN));
    }

    @Test
    public void requestAreAlwaysThrottledWhenThrottleHeaderIsNotConfigured() {
        ThrottlerImpl throttler = new ThrottlerImpl(cache, null, null);

        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");
        assertThat(throttler.throttledUntil(new Object(), request), equalTo(Instant.EPOCH));
    }

    @Test
    public void requestsAlwaysThrottledWhenAlwaysThrottleParamIsPresent() {
        ThrottlerImpl throttler = new ThrottlerImpl(cache);

        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");
        request.addParameter("throttleMeParam", "");
        assertThat(throttler.throttledUntil(new Object(), request), equalTo(Instant.MAX));
    }

    private static class ThrottlerImpl extends Throttler<Object> {

        ThrottlerImpl(Cache<Object, Object> stateStore) {
            this(stateStore, "throttleMeHeader", "throttleMeParam");
        }

        ThrottlerImpl(Cache<Object, Object> stateStore, String enableThrottlingIfHeader, String alwaysThrottleParam) {
            super(Object::new, stateStore, enableThrottlingIfHeader, alwaysThrottleParam);
        }

        @Override
        protected Instant internalThrottledUntil(Object bucket, HttpServletRequest request) {
            return Instant.EPOCH;
        }
    }
}
