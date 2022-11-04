package org.wikidata.query.rdf.blazegraph.throttling;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

public class BanThrottler <S extends BanState> extends Throttler<S> {

    private static final Logger LOG = LoggerFactory.getLogger(BanThrottler.class);

    /**
     * Constructor.
     * <p>
     * Note that a bucket represent our approximation of a single client.
     *
     * @param createThrottlingState    how to create the initial throttling state
     *                                 when we start tracking a specific client
     * @param stateStore               the cache in which we store the per client state of
     *                                 throttling
     * @param enableThrottlingIfHeader throttling is only enabled if this header is present
     * @param alwaysBanParam           this query parameter will cause banning no matter what
     */
    public BanThrottler(
            Callable<S> createThrottlingState,
            Cache<Object, S> stateStore,
            String enableThrottlingIfHeader,
            String alwaysBanParam,
            Clock clock) {
        super(createThrottlingState, stateStore, enableThrottlingIfHeader, alwaysBanParam, clock);
    }

    @Override
    protected Instant internalThrottledUntil(Object bucket, HttpServletRequest request) {
        BanState state = getStateIfPresent(bucket);
        if (state == null) return Instant.MIN;
        return state.bannedUntil();
    }

    /**
     * Should be called each time a request is throttled.
     *
     * @param bucket the bucket to which this request belongs
     * @param request the request being throttled
     */
    public void throttled(Object bucket, HttpServletRequest request) {
        if (shouldBypassThrottling(request)) return;

        try {
            getState(bucket).consumeThrottled();
        } catch (ExecutionException e) {
            LOG.warn("Could not create throttling state", e);
        }
    }

}
