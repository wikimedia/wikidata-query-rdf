package org.wikidata.query.rdf.blazegraph.throttling;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;
import org.junit.Test;

public class ThrottlingStateTest {

    @Test
    public void fullBucketsAreNotThrottled() {
        assertTrue(!new ThrottlingState(fullBucket(), fullBucket()).isThrottled());
    }

    @Test
    public void anyEmptyBucketIsThrottled() {
        assertTrue(new ThrottlingState(fullBucket(), emptyBucket()).isThrottled());
        assertTrue(new ThrottlingState(emptyBucket(), fullBucket()).isThrottled());
        assertTrue(new ThrottlingState(emptyBucket(), emptyBucket()).isThrottled());

    }

    @Test
    public void backoffDelayIsZeroWhenBucketsAreNonEmpty() {
        ThrottlingState state = new ThrottlingState(fullBucket(), fullBucket());
        assertThat(state.getBackoffDelay(), equalTo(Duration.of(0, SECONDS)));
    }

    @Test
    public void backoffDelayIsTheLargestDelay() {
        TokenBucket emptyBucket1 = mock(TokenBucket.class);
        when(emptyBucket1.getNumTokens()).thenReturn(0L);
        when(emptyBucket1.getDurationUntilNextRefill(MILLISECONDS)).thenReturn(1000L);

        TokenBucket emptyBucket2 = mock(TokenBucket.class);
        when(emptyBucket2.getNumTokens()).thenReturn(0L);
        when(emptyBucket2.getDurationUntilNextRefill(MILLISECONDS)).thenReturn(10000L);

        assertThat(
                new ThrottlingState(fullBucket(), emptyBucket1).getBackoffDelay(),
                equalTo(Duration.of(1, SECONDS)));

        assertThat(
                new ThrottlingState(emptyBucket1, emptyBucket2).getBackoffDelay(),
                equalTo(Duration.of(10, SECONDS)));
    }

    @Test
    public void canConsumeTime() {
        TokenBucket timeBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(timeBucket, fullBucket());
        long tokensBefore = timeBucket.getNumTokens();

        state.consumeTime(Duration.of(500, MILLIS));

        assertThat(timeBucket.getNumTokens() + 500, equalTo(tokensBefore));
    }

    @Test
    public void canConsumeTimeEvenIfNotEnoughTokensAvailable() {
        TokenBucket timeBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(timeBucket, fullBucket());

        state.consumeTime(Duration.of(5, SECONDS));

        assertThat(timeBucket.getNumTokens(), equalTo(0L));
    }

    @Test
    public void canConsumeErrors() {
        TokenBucket errorsBucket = fullBucket();
        ThrottlingState state = new ThrottlingState(fullBucket(), errorsBucket);
        long tokensBefore = errorsBucket.getNumTokens();

        state.consumeError();

        assertThat(errorsBucket.getNumTokens() + 1, equalTo(tokensBefore));
    }

    @Test
    public void canConsumeErrorsEvenIfNotEnoughTokensAvailable() {
        TokenBucket errorsBucket = emptyBucket();
        ThrottlingState state = new ThrottlingState(errorsBucket, fullBucket());

        state.consumeError();

        assertThat(errorsBucket.getNumTokens(), equalTo(0L));
    }

    private TokenBucket fullBucket() {
        return TokenBuckets.builder()
                .withCapacity(1000)
                .withFixedIntervalRefillStrategy(1000, 1, MINUTES)
                .build();
    }

    private TokenBucket emptyBucket() {
        TokenBucket bucket = TokenBuckets.builder()
                .withCapacity(1000)
                .withFixedIntervalRefillStrategy(1000, 1, MINUTES)
                .build();
        while (bucket.tryConsume()) {
            // consume all tokens
        }
        return bucket;
    }



}
