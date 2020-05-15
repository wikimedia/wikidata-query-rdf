package org.wikidata.query.rdf.tool.change.events;

import java.io.Serializable;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Metadata for event record.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/resource_change/1.yaml
 */
@Value
@Accessors(fluent = true)
public class EventsMeta implements Serializable {
    @JsonProperty("dt")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    Instant timestamp;
    @JsonProperty("id")
    String id;
    @JsonProperty("domain")
    String domain;
    @JsonProperty("stream")
    String stream;
    @JsonProperty("request_id")
    String requestId;

    @JsonCreator
    public EventsMeta(
            @JsonProperty("dt")
            @JsonFormat(shape = JsonFormat.Shape.STRING)
            Instant timestamp,
            @JsonProperty("id")
            String id,
            @JsonProperty("domain")
            String domain,
            @JsonProperty("stream")
            String stream,
            @JsonProperty("request_id")
            String requestId) {
        this.timestamp = timestamp;
        this.id = id;
        this.domain = domain;
        this.stream = stream;
        this.requestId = requestId;
    }
}
