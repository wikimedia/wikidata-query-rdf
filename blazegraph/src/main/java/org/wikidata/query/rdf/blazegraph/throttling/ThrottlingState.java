package org.wikidata.query.rdf.blazegraph.throttling;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;

import org.isomorphism.util.TokenBucket;

/**
 * Keeps track of resource useage for a client.
 *
 * This class tracks of the request time consumed and of the errors.
 */
public final class ThrottlingState {

    /** Bucket to track requests time. */
    private final TokenBucket timeBucket;
    /** Bucket to track errors. */
    private final TokenBucket errorsBucket;

    /**
     * Constructor.
     *
     * @param timeBucket bucket to track requests time
     * @param errorsBucket bucket to track errors
     */
    public ThrottlingState(TokenBucket timeBucket, TokenBucket errorsBucket) {
        this.timeBucket = timeBucket;
        this.errorsBucket = errorsBucket;
    }

    /**
     * Should this client be throttled.
     */
    public synchronized boolean isThrottled() {
        return timeBucket.getNumTokens() == 0 || errorsBucket.getNumTokens() == 0;
    }

    /**
     * How long should a client wait before sending the next request.
     *
     * @return 0 if the client is not throttled, or the backoff delay otherwise.
     */
    public synchronized Duration getBackoffDelay() {
        return Duration.of(
                max(backoffDelayMillis(timeBucket), backoffDelayMillis(errorsBucket)),
                MILLIS
        );
    }

    /**
     * Consumes request time from the time bucket.
     *
     * If the time bucket contains less tokens than what the request consumed,
     * all available tokens will be consumed:
     * <code>min(elapsed.toMillis(), timeBucket.getNumTokens())</code>.
     *
     * @param elapsed time elapsed during the request
     */
    public synchronized void consumeTime(Duration elapsed) {
        long tokenToConsume = min(elapsed.toMillis(), timeBucket.getNumTokens());
        // consuming zero tokens is not allowed (and would be a NOOP anyway)
        if (tokenToConsume > 0) {
            timeBucket.consume(tokenToConsume);
        }
    }

    /**
     * Consumes errors from the error bucket.
     *
     * If the error bucket is already empty, does nothing.
     */
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

}
