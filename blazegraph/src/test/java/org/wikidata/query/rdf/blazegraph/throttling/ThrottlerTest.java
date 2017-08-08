package org.wikidata.query.rdf.blazegraph.throttling;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.isomorphism.util.TokenBuckets;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.wikidata.query.rdf.blazegraph.throttling.UserAgentIpAddressBucketing.Bucket;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThrottlerTest {

    private Cache<Bucket, ThrottlingState> stateStore;
    private Throttler throttler;

    @Before
    public void createThrottlerUnderTest() {
        stateStore = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build();
        throttler = createThrottlerWithThrottlingHeader(stateStore, null);
    }

    private static Callable<ThrottlingState> createThrottlingState() {
        return () -> new ThrottlingState(
                TokenBuckets.builder()
                        .withCapacity(MILLISECONDS.convert(40, TimeUnit.SECONDS))
                        .withFixedIntervalRefillStrategy(1000000, 1, MINUTES)
                        .build(),
                TokenBuckets.builder()
                        .withCapacity(10)
                        .withFixedIntervalRefillStrategy(100, 1, MINUTES)
                        .build());
    }

    @Test
    public void newClientIsNotThrottled() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        assertFalse(throttler.isThrottled(request));
    }

    @Test
    public void shortRequestsDoNotCreateState() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(10, SECONDS));

        assertThat(stateStore.size(), equalTo(0L));
    }

    @Test
    public void backoffDelayIsZeroForNewClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        assertThat(throttler.getBackoffDelay(request), equalTo(Duration.of(0, SECONDS)));
    }

    @Test
    public void requestOverThresholdButBelowThrottlingRateEnablesUserTracking() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(30, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(throttler.isThrottled(request));
    }

    @Test
    public void requestOverThrottlingRateWillThrottleNextRequest() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(60, SECONDS));

        assertTrue(throttler.isThrottled(request));
    }

    @Test
    public void backoffDelayIsGivenForTimeThrottledClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(60, SECONDS));

        Duration backoffDelay = throttler.getBackoffDelay(request);
        assertThat(backoffDelay.compareTo(Duration.of(0, SECONDS)), greaterThan(0));
        assertThat(backoffDelay.compareTo(Duration.of(60, SECONDS)), lessThan(0));
    }

    @Test
    public void onceTrackingIsEnabledEvenShortRequestsAreTrackedAndEnableThrottling() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(30, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(throttler.isThrottled(request));

        for (int i = 0; i < 200; i++) {
            throttler.success(request, Duration.of(1, SECONDS));
        }

        assertThat(stateStore.size(), equalTo(1L));
        assertTrue(throttler.isThrottled(request));
    }

    @Test
    public void errorEnablesTrackingOfRequests() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.failure(request, Duration.of(10, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(throttler.isThrottled(request));
    }

    @Test
    public void multipleErrorsEnablesThrottling() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i < 100; i++) {
            throttler.failure(request, Duration.of(10, SECONDS));
        }

        assertThat(stateStore.size(), equalTo(1L));
        assertTrue(throttler.isThrottled(request));
    }

    @Test
    public void backoffDelayIsGivenForErrorThrottledClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i < 100; i++) {
            throttler.failure(request, Duration.of(10, SECONDS));
        }

        Duration backoffDelay = throttler.getBackoffDelay(request);
        assertThat(backoffDelay.compareTo(Duration.of(0, SECONDS)), greaterThan(0));
        assertThat(backoffDelay.compareTo(Duration.of(60, SECONDS)), lessThan(0));
    }

    @Test
    public void throttleOnlyRequestsWithThrottlingHeader() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");
        request.addHeader("do-throttle", "ignored value");

        throttler = createThrottlerWithThrottlingHeader(stateStore, "do-throttle");

        throttler.success(request, Duration.of(60, SECONDS));

        assertTrue(throttler.isThrottled(request));

        request = createRequest("UA1", "1.2.3.4");

        throttler.success(request, Duration.of(60, SECONDS));

        assertFalse(throttler.isThrottled(request));
    }

    private Throttler<Bucket> createThrottlerWithThrottlingHeader(
            Cache<Bucket, ThrottlingState> stateStore,
            String enableThrottlingIfHeader) {
        return new Throttler<>(
                Duration.of(20, SECONDS),
                new UserAgentIpAddressBucketing(),
                createThrottlingState(),
                stateStore,
                enableThrottlingIfHeader);
    }

    private MockHttpServletRequest createRequest(String userAgent, String remoteAddr) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("User-Agent", userAgent);
        request.setRemoteAddr(remoteAddr);
        return request;
    }

}
