package org.wikidata.query.rdf.blazegraph.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class SystemRuntimeMetrics {
    @JsonProperty("running_queries_before")
    private final int runningQueriesBefore;
    @JsonProperty("running_queries_after")
    private final int runningQueriesAfter;
    @JsonProperty("cpu_load_average")
    private final double cpuLoadAverage;
}
