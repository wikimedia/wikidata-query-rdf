package org.wikidata.query.rdf.blazegraph.throttling;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.blazegraph.throttling.ThrottlingTestUtils.createRequest;

import java.time.Clock;
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

public class BanThrottlerTest {

    private static final Object BUCKET_1 = new Object();

    private Cache<Object, BanState> stateStore;
    private BanThrottler<BanState> throttler;
    private ManualClock clock = new ManualClock();

    @Before
    public void createThrottlerUnderTest() {
        stateStore = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build();
        throttler = new BanThrottler<>(
                createThrottlingState(),
                stateStore,
                null,
                null,
                Clock.systemUTC());
    }

    private Callable<BanState> createThrottlingState() {
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
    public void bannedAfterTooManyThrottledRequests() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i <= 10; i++) {
            throttler.throttled(BUCKET_1, request);
        }

        assertTrue(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }


    @Test
    public void unbannedAfterBanDuration() {
        MockHttpServletRequest request = createRequest("UA1", "1.2.3.4");

        for (int i = 0; i <= 10; i++) {
            throttler.throttled(BUCKET_1, request);
        }

        assertTrue(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));

        clock.sleepUntil(throttler.throttledUntil(BUCKET_1, request));

        assertFalse(ThrottlingTestUtils.isThrottled(throttler, BUCKET_1, request, clock));
    }
}
