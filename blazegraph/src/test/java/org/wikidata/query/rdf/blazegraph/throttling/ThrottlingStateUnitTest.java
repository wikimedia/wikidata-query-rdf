package org.wikidata.query.rdf.blazegraph.throttling;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;
import org.junit.Test;
import org.wikidata.query.rdf.test.ManualClock;

public class ThrottlingStateUnitTest {

    private Duration banDuration = Duration.of(1, MINUTES);

    private ManualClock clock = new ManualClock();

    @Test
    public void fullBucketsAreNotThrottled() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket(), fullBucket(), banDuration, clock);
        assertFalse(isThrottled(state));
    }

    @Test
    public void anyEmptyBucketIsThrottled() {
        ThrottlingState state = new ThrottlingState(fullBucket(), emptyBucket(), fullBucket(), banDuration, clock);
        assertTrue(isThrottled(state));

        state = new ThrottlingState(emptyBucket(), fullBucket(), fullBucket(), banDuration, clock);
        assertTrue(isThrottled(state));

        state = new ThrottlingState(emptyBucket(), emptyBucket(), fullBucket(), banDuration, clock);
        assertTrue(isThrottled(state));

    }


    @Test
    public void backoffDelayIsTheLargestDelay() {
        TokenBucket emptyBucket1 = mock(TokenBucket.class);
        when(emptyBucket1.getNumTokens()).thenReturn(0L);
        when(emptyBucket1.getDurationUntilNextRefill(MILLISECONDS)).thenReturn(1000L);

        TokenBucket emptyBucket2 = mock(TokenBucket.class);
        when(emptyBucket2.getNumTokens()).thenReturn(0L);
        when(emptyBucket2.getDurationUntilNextRefill(MILLISECONDS)).thenReturn(10000L);

        ThrottlingState state = new ThrottlingState(fullBucket(), emptyBucket1, fullBucket(), banDuration, clock);
        assertThat(
                Duration.between(now(clock), state.throttledUntil()),
                equalTo(Duration.of(1, SECONDS)));

        state = new ThrottlingState(emptyBucket1, emptyBucket2, fullBucket(), banDuration, clock);
        assertThat(
                Duration.between(now(clock), state.throttledUntil()),
                equalTo(Duration.of(10, SECONDS)));
    }

    @Test
    public void canConsumeTime() {
        TokenBucket timeBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(timeBucket, fullBucket(), fullBucket(), banDuration);
        long tokensBefore = timeBucket.getNumTokens();

        state.consumeTime(Duration.of(500, MILLIS));

        assertThat(timeBucket.getNumTokens() + 500, equalTo(tokensBefore));
    }

    @Test
    public void canConsumeTimeEvenIfNotEnoughTokensAvailable() {
        TokenBucket timeBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(timeBucket, fullBucket(), fullBucket(), banDuration);

        state.consumeTime(Duration.of(5, SECONDS));

        assertThat(timeBucket.getNumTokens(), equalTo(0L));
    }

    @Test
    public void canConsumeErrors() {
        TokenBucket errorsBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(fullBucket(), errorsBucket, fullBucket(), banDuration);
        long tokensBefore = errorsBucket.getNumTokens();

        state.consumeError();

        assertThat(errorsBucket.getNumTokens() + 1, equalTo(tokensBefore));
    }

    @Test
    public void canConsumeErrorsEvenIfNotEnoughTokensAvailable() {
        TokenBucket errorsBucket = emptyBucket();
        ThrottlingState state = new ThrottlingState(errorsBucket, fullBucket(), fullBucket(), banDuration);

        state.consumeError();

        assertThat(errorsBucket.getNumTokens(), equalTo(0L));
    }

    @Test
    public void notBannedWhenBucketIsFull() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket(), fullBucket(), banDuration, clock);

        state.consumeThrottled();
        assertFalse(isBanned(state));
    }

    @Test
    public void throttledButNotBanned() {
        ThrottlingState state = new ThrottlingState(emptyBucket(), emptyBucket(), fullBucket(), banDuration, clock);

        state.consumeError();
        state.consumeThrottled();

        assertFalse(isBanned(state));
        assertTrue(isThrottled(state));
    }

    @Test
    public void bannedButNotThrottled() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket(), emptyBucket(), banDuration, clock);

        state.consumeError();
        state.consumeThrottled();

        assertTrue(isBanned(state));
        assertFalse(isThrottled(state));
    }

    @Test
    public void banDurationIsSetCorrectly() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket(), emptyBucket(), banDuration, clock);

        state.consumeThrottled();

        // AssertJ would have nicer assertions for dates...
        assertThat(
                Duration.between(now(clock), state.bannedUntil()),
                equalTo(Duration.of(1, MINUTES)));
    }

    @Test
    public void banIsRemovedOnceExpired() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket(), emptyBucket(), banDuration, clock);

        state.consumeThrottled();

        assertTrue(isBanned(state));

        clock.sleepUntil(state.bannedUntil().plusMillis(1));

        assertFalse(isBanned(state));
    }

    private TokenBucket fullBucket() {
        return TokenBuckets.builder()
                .withCapacity(1000)
                .withFixedIntervalRefillStrategy(1000, 1, TimeUnit.MINUTES)
                .build();
    }

    private TokenBucket emptyBucket() {
        TokenBucket bucket = TokenBuckets.builder()
                .withCapacity(1000)
                .withFixedIntervalRefillStrategy(1000, 1, TimeUnit.MINUTES)
                .build();
        while (bucket.tryConsume()) {
            // consume all tokens
        }
        return bucket;
    }

    private boolean isThrottled(ThrottlingState state) {
        return state.throttledUntil().isAfter(now(clock));
    }

    private boolean isBanned(ThrottlingState state) {
        return state.bannedUntil().isAfter(now(clock));
    }

}
