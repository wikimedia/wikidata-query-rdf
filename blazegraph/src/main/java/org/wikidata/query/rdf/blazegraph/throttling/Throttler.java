package org.wikidata.query.rdf.blazegraph.throttling;

import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.time.temporal.ChronoUnit.MILLIS;

/**
 * Implement throttling logic.
 *
 * @see ThrottlingFilter for a more complete description of how throttling
 * works.
 * @param <B> type of the bucket used to differentiate clients
 */
public class Throttler<B> {

    private static final Logger log = LoggerFactory.getLogger(Throttler.class);

    /** How to associate a request with a specific bucket. */
    private final Bucketing<B> bucketing;
    /**
     * Stores the throttling state by buckets.
     *
     * This is a slight abuse of Guava {@link Cache}, but makes it easy to have
     * an LRU map with an automatic cleanup mechanism.
     */
    // TODO: we probably want to expose metrics on the size / usage of this cache
    private final Cache<B, ThrottlingState> state;
    /** Requests longer than this will trigger tracking resource consumption. */
    private final Duration requestTimeThreshold;
    /** How to create the initial throttling state when we start tracking a specific client. */
    private final Callable<ThrottlingState> createThrottlingState;

    /**
     * Constructor.
     *
     * Note that a bucket represent our approximation of a single client.
     *
     * @param requestTimeThreshold requests longer than this will trigger
     *                             tracking resource consumption
     * @param bucketing how to associate a request with a specific bucket
     * @param createThrottlingState how to create the initial throttling state
     *                              when we start tracking a specific client
     * @param stateStore the cache in which we store the per client state of
     *                   throttling
     */
    public Throttler(
            Duration requestTimeThreshold,
            Bucketing<B> bucketing,
            Callable<ThrottlingState> createThrottlingState,
            Cache<B, ThrottlingState> stateStore) {
        this.requestTimeThreshold = requestTimeThreshold;
        this.bucketing = bucketing;
        this.state = stateStore;
        this.createThrottlingState = createThrottlingState;
    }

    /**
     * Should this request be throttled.
     *
     * @param request the request to check
     * @return true if the request should be throttled
     */
    public boolean isThrottled(HttpServletRequest request) {
        ThrottlingState throttlingState = state.getIfPresent(bucketing.bucket(request));
        if (throttlingState == null) return false;

        return throttlingState.isThrottled();
    }

    /**
     * Notify this throttler that a request has been completed successfully.
     *
     * @param request the request
     * @param elapsed how long that request took
     */
    public void success(HttpServletRequest request, Duration elapsed) {
        try {
            B bucket = bucketing.bucket(request);
            ThrottlingState throttlingState;
            // only start to keep track of time usage if requests are expensive
            if (elapsed.compareTo(requestTimeThreshold) > 0) {
                throttlingState = state.get(bucket, createThrottlingState);
            } else {
                throttlingState = state.getIfPresent(bucket);
            }
            if (throttlingState != null) {
                throttlingState.consumeTime(elapsed);
            }
        } catch (ExecutionException ee) {
            log.warn("Could not create throttling state", ee);
        }
    }

    /**
     * Notify this throttler that a request has completed in error.
     *
     * @param request the request
     * @param elapsed how long that request took
     */
    public void failure(HttpServletRequest request, Duration elapsed) {
        try {
            ThrottlingState throttlingState = state.get(bucketing.bucket(request), createThrottlingState);

            throttlingState.consumeError();
            throttlingState.consumeTime(elapsed);
        } catch (ExecutionException ee) {
            log.warn("Could not create throttling state", ee);
        }
    }

    /**
     * How long should this client wait before his next request.
     *
     * @param request the request
     * @return 0 if no throttling, the backoff delay otherwise
     */
    public Duration getBackoffDelay(HttpServletRequest request) {
        ThrottlingState throttlingState = state.getIfPresent(bucketing.bucket(request));
        if (throttlingState == null) return Duration.of(0, MILLIS);

        return throttlingState.getBackoffDelay();
    }

}
