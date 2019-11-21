package org.wikidata.query.rdf.blazegraph.throttling;

public interface SystemOverloadFilterMXBean {

    /**
     * Number of requests that have been rejected since the start of this filter.
     */
    long getRejectedCount();

}
