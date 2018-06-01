package org.wikidata.query.rdf.blazegraph.throttling;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.blazegraph.throttling.ThrottlingTestUtils.createRequest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.isomorphism.util.TokenBuckets;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.wikidata.query.rdf.test.ManualClock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class TimeAndErrorsThrottlerTest {

    private static final Object BUCKET_1 = new Object();

    private Cache<Object, ThrottlingState> stateStore;
    private TimeAndErrorsThrottler<ThrottlingState> throttler;
    private ManualClock clock = new ManualClock();

    @Before
    public void createThrottlerUnderTest() {
        stateStore = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build();
        throttler = new TimeAndErrorsThrottler(
                Duration.of(20, SECONDS),
                createThrottlingState(),
                stateStore,
                null,
                null);
    }

    private Callable<ThrottlingState> createThrottlingState() {
        return () -> new ThrottlingState(
                TokenBuckets.builder()
                        .withCapacity(MILLISECONDS.convert(40, TimeUnit.SECONDS))
                        .withFixedIntervalRefillStrategy(1_000_000, 1, MINUTES)
                        .build(),
                TokenBuckets.builder()
                        .withCapacity(10)
                        .withFixedIntervalRefillStrategy(100, 1, MINUTES)
                        .build(),
                TokenBuckets.builder()
                        .withCapacity(10)
                        .withFixedIntervalRefillStrategy(10, 1, MINUTES)
                        .build(),
                Duration.of(10, ChronoUnit.MINUTES),
                clock);
    }

    @Test
    public void newClientIsNotThrottled() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        assertFalse(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void shortRequestsDoNotCreateState() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(BUCKET_1, request, Duration.of(10, SECONDS));

        assertThat(stateStore.size(), equalTo(0L));
    }

    @Test
    public void backoffDelayIsInThePastForNewClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        assertTrue(throttler.throttledUntil(BUCKET_1, request).isBefore(now(clock)));
    }

    @Test
    public void requestOverThresholdButBelowThrottlingRateEnablesUserTracking() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(BUCKET_1, request, Duration.of(30, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void requestOverThrottlingRateWillThrottleNextRequest() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(BUCKET_1, request, Duration.of(60, SECONDS));

        assertTrue(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void backoffDelayIsGivenForTimeThrottledClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(BUCKET_1, request, Duration.of(60, SECONDS));

        Duration backoffDelay = Duration.between(now(clock), throttler.throttledUntil(BUCKET_1, request));

        assertThat(backoffDelay.compareTo(Duration.of(0, SECONDS)), greaterThan(0));
        assertThat(backoffDelay.compareTo(Duration.of(60, SECONDS)), lessThan(0));
    }

    @Test
    public void onceTrackingIsEnabledEvenShortRequestsAreTrackedAndEnableThrottling() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.success(BUCKET_1, request, Duration.of(30, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));

        for (int i = 0; i < 200; i++) {
            throttler.success(BUCKET_1, request, Duration.of(1, SECONDS));
        }

        assertThat(stateStore.size(), equalTo(1L));
        assertTrue(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void errorEnablesTrackingOfRequests() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        throttler.failure(BUCKET_1, request, Duration.of(10, SECONDS));

        assertThat(stateStore.size(), equalTo(1L));
        assertFalse(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void multipleErrorsEnablesThrottling() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i < 100; i++) {
            throttler.failure(BUCKET_1, request, Duration.of(10, SECONDS));
        }

        assertThat(stateStore.size(), equalTo(1L));
        assertTrue(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }

    @Test
    public void backoffDelayIsGivenForErrorThrottledClient() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i < 100; i++) {
            throttler.failure(BUCKET_1, request, Duration.of(10, SECONDS));
        }

        Duration backoffDelay = Duration.between(now(clock), throttler.throttledUntil(BUCKET_1, request));
        assertThat(backoffDelay.compareTo(Duration.of(0, SECONDS)), greaterThan(0));
        assertThat(backoffDelay.compareTo(Duration.of(60, SECONDS)), lessThan(0));
    }

}
