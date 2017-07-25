package org.wikidata.query.rdf.blazegraph.throttling;

import javax.servlet.http.HttpServletRequest;

/**
 * Segmentation of requests.
 *
 * Resource consumption is done by <i>client</i>. This interface defines how we
 * segment clients in different buckets.
 *
 * @param <T> the type of the bucket identifier
 */
public interface Bucketing<T> {
    /**
     * Compute a identifier for the bucket in which this request needs to be
     * stored.
     *
     * @param request the request for which to compute the bucket
     * @return an object identifying the bucket
     */
    T bucket(HttpServletRequest request);
}
