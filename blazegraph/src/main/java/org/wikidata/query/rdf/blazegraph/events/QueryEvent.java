package org.wikidata.query.rdf.blazegraph.events;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.time.Duration;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Event describing a sparql query.
 *
 * https://gerrit.wikimedia.org/r/plugins/gitiles/mediawiki/event-schemas/+/master/jsonschema/sparql/query/1.3.0.yaml
 */
@JsonPropertyOrder({
    "$schema", "meta", "http", "backend_host", "graph_name",
    "namespace", "query", "format", "params", "query_time",
    "system_runtime_metrics", "performer"
})
public class QueryEvent implements Event {
    private static final String SCHEMA = "/sparql/query/1.3.0";
    private final EventMetadata metadata;
    private final EventHttpMetadata httpMetadata;
    private final String backendHost;
    private final String graphName;
    private final String namespace;
    private final String query;
    private final String format;
    private final Map<String, String> params;
    private final Duration queryTime;
    private final SystemRuntimeMetrics systemRuntimeMetrics;
    private final EventPerformer performer;

    public QueryEvent(EventMetadata metadata,
                      EventHttpMetadata http,
                      String backendHost,
                      @Nullable String graphName,
                      String namespace,
                      String query,
                      @Nullable String format,
                      Map<String, String> params,
                      Duration queryTime,
                      SystemRuntimeMetrics systemRuntimeMetrics,
                      @Nullable EventPerformer performer
    ) {
        this.metadata = metadata;
        this.httpMetadata = http;
        this.backendHost = backendHost;
        this.graphName = graphName;
        this.namespace = namespace;
        this.query = query;
        this.format = format;
        this.params = params;
        this.queryTime = queryTime;
        this.systemRuntimeMetrics = systemRuntimeMetrics;
        this.performer = performer;
    }

    public EventMetadata getMetadata() {
        return metadata;
    }

    @JsonProperty("$schema")
    public String getSchema() {
        return SCHEMA;
    }

    @JsonProperty("http")
    public EventHttpMetadata getHttpMetadata() {
        return httpMetadata;
    }

    @JsonProperty("backend_host")
    public String getBackendHost() {
        return backendHost;
    }

    @JsonProperty("graph_name")
    @JsonInclude(NON_NULL)
    public String getGraphName() {
        return graphName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getQuery() {
        return query;
    }

    @JsonInclude(NON_NULL)
    public String getFormat() {
        return format;
    }

    public Map<String, String> getParams() {
        return params;
    }

    @JsonProperty("query_time")
    public long getQueryTime() {
        return queryTime.toMillis();
    }

    @JsonProperty("system_runtime_metrics")
    SystemRuntimeMetrics getSystemRuntimeMetrics() {
        return systemRuntimeMetrics;
    }

    @JsonInclude(NON_NULL)
    public EventPerformer getPerformer() {
        return performer;
    }
}
