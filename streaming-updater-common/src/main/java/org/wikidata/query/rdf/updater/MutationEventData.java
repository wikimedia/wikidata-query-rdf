package org.wikidata.query.rdf.updater;

import java.io.Serializable;
import java.time.Instant;

import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import com.fasterxml.jackson.annotation.JsonCreator;
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
    @JsonSubTypes.Type(value = DiffEventData.class, name = "reconcile"),
    @JsonSubTypes.Type(value = MutationEventData.class, name = "delete"),
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
    public static final String DELETE_OPERATION = "delete";
    public static final String RECONCILE_OPERATION = "reconcile";
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

    @JsonCreator
    public MutationEventData(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("entity") String entity,
            @JsonProperty("revision") long revision,
            @JsonProperty("event_time") Instant eventTime,
            @JsonProperty("sequence") int sequence,
            @JsonProperty("sequence_length") int sequenceLength,
            @JsonProperty("operation") String operation
    ) {
        this.meta = meta;
        this.entity = entity;
        this.revision = revision;
        this.eventTime = eventTime;
        this.sequence = sequence;
        this.sequenceLength = sequenceLength;
        this.operation = operation;
    }
}
