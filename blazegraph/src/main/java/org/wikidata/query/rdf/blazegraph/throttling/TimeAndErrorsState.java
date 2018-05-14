package org.wikidata.query.rdf.blazegraph.throttling;

import java.time.Duration;
import java.time.Instant;

/**
 * Keeps track of each client for {@link TimeAndErrorsThrottler}.
 */
public interface TimeAndErrorsState {
    /**
     * Consumes request time from the time bucket.
     *
     * If the time bucket contains less tokens than what the request consumed,
     * all available tokens will be consumed:
     * <code>min(elapsed.toMillis(), timeBucket.getNumTokens())</code>.
     *
     * @param elapsed time elapsed during the request
     */
    void consumeTime(Duration elapsed);

    /**
     * Consumes errors from the error bucket.
     *
     * If the error bucket is already empty, does nothing.
     */
    void consumeError();

    /**
     * A user is throttled until this instant.
     *
     * @return the end of the throttling if the user is throttled, an {@link Instant} in the past otherwise
     */
    Instant throttledUntil();
}
