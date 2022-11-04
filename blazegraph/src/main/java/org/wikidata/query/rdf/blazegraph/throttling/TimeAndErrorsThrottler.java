package org.wikidata.query.rdf.blazegraph.throttling;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

/**
 * Throttle users based on the cumulative request processing time and number of errors.
 */
public class TimeAndErrorsThrottler<S extends TimeAndErrorsState> extends Throttler<S> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeAndErrorsThrottler.class);

    /** Requests longer than this will trigger tracking resource consumption. */
    private final Duration requestTimeThreshold;


    /**
     * Constructor.
     *
     * Note that a bucket represent our approximation of a single client.
     *
     * @param requestTimeThreshold     requests longer than this will trigger
     *                                 tracking resource consumption
     * @param createThrottlingState    how to create the initial throttling state
     *                                 when we start tracking a specific client
     * @param stateStore               the cache in which we store the per client state of
     *                                 throttling
     * @param enableThrottlingIfHeader throttling is only enabled if this header is present
     * @param alwaysThrottleParam      this query parameter will cause throttling no matter what
     */
    public TimeAndErrorsThrottler(
            Duration requestTimeThreshold,
            Callable<S> createThrottlingState,
            Cache<Object, S> stateStore,
            String enableThrottlingIfHeader,
            String alwaysThrottleParam,
            Clock clock) {
        super(createThrottlingState, stateStore, enableThrottlingIfHeader, alwaysThrottleParam, clock);

        this.requestTimeThreshold = requestTimeThreshold;
    }

    /**
     * Notify this throttler that a request has been completed successfully.
     *
     * @param bucket the bucket to which this request belongs
     * @param request the request
     * @param elapsed how long that request took
     */
    public void success(Object bucket, HttpServletRequest request, Duration elapsed) {
        if (shouldBypassThrottling(request)) {
            return;
        }
        try {
            S state;
            // only start to keep track of time usage if requests are expensive
            if (elapsed.compareTo(requestTimeThreshold) > 0) {
                state = getState(bucket);
            } else {
                state = getStateIfPresent(bucket);
            }
            if (state != null) {
                state.consumeTime(elapsed);
            }
        } catch (ExecutionException e) {
            LOG.warn("Could not create throttling state", e);
        }
    }

    /**
     * Notify this throttler that a request has completed in error.
     *
     * @param bucket the bucket to which this request belongs
     * @param request the request
     * @param elapsed how long that request took
     */
    public void failure(Object bucket, HttpServletRequest request, Duration elapsed) {
        if (shouldBypassThrottling(request)) return;

        try {
            S state = getState(bucket);

            state.consumeError();
            state.consumeTime(elapsed);
        } catch (ExecutionException e) {
            LOG.warn("Could not create throttling state", e);
        }
    }

    @Override
    protected Instant internalThrottledUntil(Object bucket, HttpServletRequest request) {
        S state = getStateIfPresent(bucket);
        if (state == null) return Instant.MIN;
        return state.throttledUntil();
    }
}
