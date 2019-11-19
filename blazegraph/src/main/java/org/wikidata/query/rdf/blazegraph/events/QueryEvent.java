package org.wikidata.query.rdf.blazegraph.events;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.time.Duration;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Event describing a sparql query.
 *
 * https://gerrit.wikimedia.org/r/plugins/gitiles/mediawiki/event-schemas/+/master/jsonschema/sparql/query/1.0.0.yaml
 */
public class QueryEvent implements Event {
    private static final String SCHEMA = "/sparql/query/1.0.0";
    private final EventMetadata metadata;
    private final EventHttpMetadata httpMetadata;
    private final String backendHost;
    private final String namespace;
    private final String query;
    private final String format;
    private final Map<String, String> params;
    private final Duration queryTime;

    public QueryEvent(EventMetadata metadata, EventHttpMetadata http, String backendHost, String namespace, String query,
                      @Nullable String format, Map<String, String> params, Duration queryTime) {
        this.metadata = metadata;
        this.httpMetadata = http;
        this.backendHost = backendHost;
        this.namespace = namespace;
        this.query = query;
        this.format = format;
        this.params = params;
        this.queryTime = queryTime;
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
}
