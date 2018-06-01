package org.wikidata.query.rdf.blazegraph.throttling;

/**
 * Expose statistics about throttling filter.
 */
public interface ThrottlingMXBean {

    /**
     * The number of entries in the state store.
     *
     * This represents the number of clients currently being tracked.
     */
    long getStateSize();

    /**
     * Monotonic increasing counter of the the number of requests that have been throttled.
     */
    long getNumberOfThrottledRequests();

    /**
     * Monotonic increasing counter of the the number of requests that have been banned.
     */
    long getNumberOfBannedRequests();

}
