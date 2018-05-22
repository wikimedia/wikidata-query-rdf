package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.base.Strings.emptyToNull;
import static java.time.temporal.ChronoUnit.MILLIS;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

/**
 * Implement throttling logic.
 *
 * @see ThrottlingFilter for a more complete description of how throttling
 * works.
 */
public class Throttler {

    private static final Logger log = LoggerFactory.getLogger(Throttler.class);

    /**
     * Stores the throttling state by buckets.
     *
     * This is a slight abuse of Guava {@link Cache}, but makes it easy to have
     * an LRU map with an automatic cleanup mechanism.
     */
    private final Cache<Object, ThrottlingState> state;
    /** Requests longer than this will trigger tracking resource consumption. */
    private final Duration requestTimeThreshold;
    /** How to create the initial throttling state when we start tracking a specific client. */
    private final Callable<ThrottlingState> createThrottlingState;

    /**
     * Throttling is only enabled if this header is set.
     *
     * This can be used to throttle only request coming through a revers proxy,
     * which will set this specific header. Only the presence of the header is
     * checked, not its value.
     *
     * If <code>null</code>, all requests will be throttled.
     */
    private final String enableThrottlingIfHeader;

    /**
     * This parameter in query will cause throttling no matter what.
     *
     * This can be used for testing.
     */
    public final String alwaysThrottleParam;

    /**
     * Constructor.
     *
     * Note that a bucket represent our approximation of a single client.
     *
     * @param requestTimeThreshold requests longer than this will trigger
     *                             tracking resource consumption
     * @param createThrottlingState how to create the initial throttling state
     *                              when we start tracking a specific client
     * @param stateStore the cache in which we store the per client state of
     *                   throttling
     * @param enableThrottlingIfHeader throttling is only enabled if this header is present
     * @param alwaysThrottleParam this query parameter will cause throttling no matter what
     */
    public Throttler(
            Duration requestTimeThreshold,
            Callable<ThrottlingState> createThrottlingState,
            Cache<Object, ThrottlingState> stateStore,
            String enableThrottlingIfHeader,
            String alwaysThrottleParam) {
        this.requestTimeThreshold = requestTimeThreshold;
        this.state = stateStore;
        this.createThrottlingState = createThrottlingState;
        this.enableThrottlingIfHeader = emptyToNull(enableThrottlingIfHeader);
        this.alwaysThrottleParam = emptyToNull(alwaysThrottleParam);
    }

    /**
     * The size (in number of entries) of the throttling state.
     *
     * This correspond to the number of clients currently being tracked.
     *
     * @return the number of entries in state size
     */
    public long getStateSize() {
        return state.size();
    }

    /**
     * Should this request be throttled.
     *
     * @param bucket the bucket to which this request belongs
     * @param request the request to check
     * @return true if the request should be throttled
     */
    public boolean isThrottled(Object bucket, HttpServletRequest request) {
        if (alwaysThrottle(request)) return true;
        if (shouldBypassThrottling(request)) return false;

        ThrottlingState throttlingState = state.getIfPresent(bucket);
        if (throttlingState == null) return false;

        return throttlingState.isThrottled();
    }

    private boolean alwaysThrottle(HttpServletRequest request) {
        if (alwaysThrottleParam == null) return false;

        return request.getParameter(alwaysThrottleParam) != null;
    }

    /**
     * Check whether this request should have throttling enabled.
     * @param request
     * @return if true then skip throttling
     */
    private boolean shouldBypassThrottling(HttpServletRequest request) {
        if (enableThrottlingIfHeader == null) return false;

        return request.getHeader(enableThrottlingIfHeader) == null;
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
     * @param bucket the bucket to which this request belongs
     * @param request the request
     * @param elapsed how long that request took
     */
    public void failure(Object bucket, HttpServletRequest request, Duration elapsed) {
        if (shouldBypassThrottling(request)) {
            return;
        }
        try {
            ThrottlingState throttlingState = state.get(bucket, createThrottlingState);

            throttlingState.consumeError();
            throttlingState.consumeTime(elapsed);
        } catch (ExecutionException ee) {
            log.warn("Could not create throttling state", ee);
        }
    }

    /**
     * How long should this client wait before his next request.
     *
     * @param bucket the bucket to which this request belongs
     * @param request the request
     * @return 0 if no throttling, the backoff delay otherwise
     */
    public Duration getBackoffDelay(Object bucket, HttpServletRequest request) {
        ThrottlingState throttlingState = state.getIfPresent(bucket);
        if (throttlingState == null) return Duration.of(0, MILLIS);

        return throttlingState.getBackoffDelay();
    }

}
