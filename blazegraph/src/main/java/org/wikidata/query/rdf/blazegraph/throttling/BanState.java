package org.wikidata.query.rdf.blazegraph.throttling;

import java.time.Instant;

/**
 * Keeps track of each client for {@link BanThrottler}.
 */
public interface BanState {
    /**
     * Consume throttled requests.
     *
     * Should be called each time a request is throttled. Will update the banned state.
     */
    void consumeThrottled();

    /**
     * A user is banned until this instant.
     *
     * @return the end of the ban if the user is banned, an {@link Instant} in the past otherwise
     */
    Instant bannedUntil();
}
