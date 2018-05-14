package org.wikidata.query.rdf.blazegraph.throttling;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.isomorphism.util.TokenBucket;

import com.google.common.annotations.VisibleForTesting;

/**
 * Keeps track of resource useage for a client.
 *
 * This class tracks of the request time consumed, the errors and the number of
 * request throttled for a given client.
 */
@ThreadSafe
public final class ThrottlingState implements BanState, TimeAndErrorsState {

    private final Ban ban;
    private final TimeAndErrors timeAndErrors;

    /**
     * Constructor.
     *
     * @param timeBucket bucket to track requests time
     * @param errorsBucket bucket to track errors
     * @param throttleBucket bucket to track throttling
     * @param banDuration default duration of a ban
     */
    public ThrottlingState(TokenBucket timeBucket, TokenBucket errorsBucket, TokenBucket throttleBucket,
                           Duration banDuration) {
        this(timeBucket, errorsBucket, throttleBucket, banDuration, Clock.systemUTC());
    }

    /**
     * Constructor.
     *
     * @param timeBucket bucket to track requests time
     * @param errorsBucket bucket to track errors
     * @param throttleBucket bucket to track throttling
     * @param banDuration default duration of a ban
     * @param clock clock injected for testing
     */
    @VisibleForTesting
    ThrottlingState(TokenBucket timeBucket, TokenBucket errorsBucket, TokenBucket throttleBucket,
                    Duration banDuration, Clock clock) {
        this.ban = new Ban(throttleBucket, banDuration, clock);
        this.timeAndErrors = new TimeAndErrors(timeBucket, errorsBucket, clock);
    }

    @Override
    public void consumeThrottled() {
        ban.consumeThrottled();
    }

    @Override
    public Instant bannedUntil() {
        return ban.bannedUntil();
    }

    @Override
    public void consumeTime(Duration elapsed) {
        timeAndErrors.consumeTime(elapsed);
    }

    @Override
    public void consumeError() {
        timeAndErrors.consumeError();
    }

    @Override
    public Instant throttledUntil() {
        return timeAndErrors.throttledUntil();
    }

    private static final class Ban implements BanState {

        /** Bucket to track throttling. */
        @Nonnull private final TokenBucket throttleBucket;

        /**
         * Instant until which a user is banned.
         *
         * If this instant is in the past, the user is not throttled.
         */
        @Nonnull
        private Instant bannedUntil = Instant.MIN;

        /**
         * The full duration of a new ban.
         *
         * This is configuration of the state and is used to compute bannedUntil
         * when a ban is activated.
         */
        @Nonnull
        private final Duration banDuration;

        @Nonnull
        private final Clock clock;

        private Ban(@Nonnull TokenBucket throttleBucket, @Nonnull Duration banDuration, @Nonnull Clock clock) {
            this.throttleBucket = throttleBucket;
            this.banDuration = banDuration;
            this.clock = clock;
        }

        @Override
        public synchronized void consumeThrottled() {
            // don't consume any token if we are already banned
            if (isBanned()) return;

            if (!throttleBucket.tryConsume()) {
                // all available throttling has been consumed, let's activate a ban
                bannedUntil = now(clock).plus(banDuration);
            }
        }

        private boolean isBanned() {
            return bannedUntil.isAfter(now(clock));
        }

        @Override
        public synchronized Instant bannedUntil() {
            return bannedUntil;
        }
    }

    private static final class TimeAndErrors implements TimeAndErrorsState {

        /** Bucket to track requests time. */
        @Nonnull private final TokenBucket timeBucket;
        /** Bucket to track errors. */
        @Nonnull private final TokenBucket errorsBucket;

        @Nonnull
        private final Clock clock;

        private TimeAndErrors(@Nonnull TokenBucket timeBucket, @Nonnull TokenBucket errorsBucket, @Nonnull Clock clock) {
            this.timeBucket = timeBucket;
            this.errorsBucket = errorsBucket;
            this.clock = clock;
        }

        @Override
        public synchronized void consumeTime(Duration elapsed) {
            long tokenToConsume = min(elapsed.toMillis(), timeBucket.getNumTokens());
            // consuming zero tokens is not allowed (and would be a NOOP anyway)
            if (tokenToConsume > 0) {
                timeBucket.consume(tokenToConsume);
            }
        }

        @Override
        public synchronized void consumeError() {
            errorsBucket.tryConsume();
        }

        /**
         * How long to wait before next refill for the specified bucket.
         * @param bucket the bucket to check
         * @return 0 if the bucket is not empty, the delay otherwise
         */
        private static long backoffDelayMillis(TokenBucket bucket) {
            if (bucket.getNumTokens() > 0) return 0;
            return bucket.getDurationUntilNextRefill(MILLISECONDS);
        }

        @Override
        public synchronized Instant throttledUntil() {
            if (!(timeBucket.getNumTokens() <= 0 || errorsBucket.getNumTokens() <= 0)) return Instant.MIN;

            return now(clock).plus(Duration.of(
                    max(backoffDelayMillis(timeBucket), backoffDelayMillis(errorsBucket)),
                    MILLIS
            ));
        }

    }
}
