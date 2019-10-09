package org.wikidata.query.rdf.blazegraph.events;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Commons metadata.
 *
 * https://gerrit.wikimedia.org/r/plugins/gitiles/mediawiki/event-schemas/+/master/jsonschema/common/1.0.0.yaml
 */
@JsonPropertyOrder({"id", "dt", "request_id", "domain", "stream"})
public class EventMetadata {
    private final String id;
    private final Instant dt;
    /**
     * Unique ID of the request that caused the event.
     */
    private final String requestId;
    /**
     * Domain the event or entity pertains to.
     */
    private final String domain;
    /**
     * Name of the stream/queue/dataset that this event belongs in.
     */
    private final String stream;

    public EventMetadata(String requestId, String id, Instant dt, String domain, String stream) {
        this.id = id;
        this.dt = dt;
        this.requestId = requestId;
        this.domain = domain;
        this.stream = stream;
    }

    public String getId() {
        return id;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public Instant getDt() {
        return dt;
    }

    @JsonProperty("request_id")
    public String getRequestId() {
        return requestId;
    }

    public String getDomain() {
        return domain;
    }

    public String getStream() {
        return stream;
    }
}
