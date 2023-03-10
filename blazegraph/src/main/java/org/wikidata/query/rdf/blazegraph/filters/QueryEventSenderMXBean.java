package org.wikidata.query.rdf.blazegraph.filters;

public interface QueryEventSenderMXBean {
    int getRunningQueries();
    long getStartedQueries();
}
