package org.wikidata.query.rdf.tool.stream;

import java.io.Serializable;
import java.time.Instant;

import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Value;
import lombok.experimental.NonFinal;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "operation",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DiffEventData.class, name = "diff"),
        @JsonSubTypes.Type(value = DiffEventData.class, name = "import"),
})
@Value
@NonFinal
@JsonPropertyOrder(value = {
        "$schema",
        "meta",
        "entity",
        "revision",
        "event_time",
        "sequence",
        "sequence_length",
        "operation",
})
public class MutationEventData implements Serializable {
    public static final String DIFF_OPERATION = "diff";
    public static final String IMPORT_OPERATION = "import";
    private static final String SCHEMA = "/wikibase/rdf/update_stream/1.0.0";

    @JsonProperty("$schema")
    String schema = SCHEMA;
    @JsonProperty("meta")
    EventsMeta meta;
    String entity;
    long revision;
    @JsonProperty("event_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    Instant eventTime;
    int sequence;
    @JsonProperty("sequence_length")
    int sequenceLength;
    @JsonProperty("operation")
    String operation;
}
