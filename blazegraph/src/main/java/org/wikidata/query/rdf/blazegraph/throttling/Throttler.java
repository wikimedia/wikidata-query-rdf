package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.base.Strings.emptyToNull;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.http.HttpServletRequest;

import com.google.common.cache.Cache;

/**
 * Implement throttling logic.
 *
 * @see ThrottlingFilter for a more complete description of how throttling
 * works.
 *
 * @param <S> the type of state used by child classes to track the clients.
 */
@ThreadSafe
public abstract class Throttler<S> {

    /**
     * Stores the throttling state by buckets.
     *
     * This is a slight abuse of Guava {@link Cache}, but makes it easy to have
     * an LRU map with an automatic cleanup mechanism.
     */
    private final Cache<Object, S> state;

    /** How to create the initial throttling state when we start tracking a specific client. */
    private final Callable<S> createThrottlingState;

    /**
     * Throttling is only enabled if this header is set.
     *
     * This can be used to throttle only request coming through a revers proxy,
     * which will set this specific header. Only the presence of the header is
     * checked, not its value.
     *
     * If <code>null</code>, all requests will be throttled.
     */
    @Nullable
    private final String enableThrottlingIfHeader;

    /**
     * This parameter in query will cause throttling no matter what.
     *
     * This can be used for testing.
     */
    @Nullable
    public final String alwaysThrottleParam;

    /**
     * Constructor.
     *
     * Note that a bucket represent our approximation of a single client.
     * @param createThrottlingState how to create the initial throttling state
     *                              when we start tracking a specific client
     * @param stateStore the cache in which we store the per client state of
     *                   throttling
     * @param enableThrottlingIfHeader throttling is only enabled if this header is present
     * @param alwaysThrottleParam this query parameter will cause throttling no matter what
     */
    public Throttler(
            Callable<S> createThrottlingState,
            Cache<Object, S> stateStore,
            String enableThrottlingIfHeader,
            String alwaysThrottleParam) {
        this.state = stateStore;
        this.createThrottlingState = createThrottlingState;
        this.enableThrottlingIfHeader = emptyToNull(enableThrottlingIfHeader);
        this.alwaysThrottleParam = emptyToNull(alwaysThrottleParam);
    }

    protected S getState(Object bucket) throws ExecutionException {
        return state.get(bucket, createThrottlingState);
    }

    protected S getStateIfPresent(Object bucket) {
        return state.getIfPresent(bucket);
    }

    private boolean alwaysThrottle(HttpServletRequest request) {
        if (alwaysThrottleParam == null) return false;

        return request.getParameter(alwaysThrottleParam) != null;
    }

    /**
     * Check whether this request should have throttling enabled.
     *
     * @return true if throttling should be skipped
     */
    protected boolean shouldBypassThrottling(HttpServletRequest request) {
        if (enableThrottlingIfHeader == null) return false;

        return request.getHeader(enableThrottlingIfHeader) == null;
    }

    /**
     * Until when is this request throttled.
     *
     * @return the end time of the throttling if the request is throttled, a time in the past if the request isn't throttled
     */
    public Instant throttledUntil(Object bucket, HttpServletRequest request) {
        if (alwaysThrottle(request)) return Instant.MAX;
        if (shouldBypassThrottling(request)) return Instant.MIN;

        return internalThrottledUntil(bucket, request);
    }

    /**
     * Implemented by clients for the specific throttling logic.
     *
     * @see Throttler#throttledUntil(Object, HttpServletRequest)
     */
    protected abstract Instant internalThrottledUntil(Object bucket, HttpServletRequest request);

}
