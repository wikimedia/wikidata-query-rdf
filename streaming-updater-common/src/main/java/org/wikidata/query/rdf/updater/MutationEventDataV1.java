package org.wikidata.query.rdf.updater;

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
    @JsonSubTypes.Type(value = DiffEventDataV1.class, name = MutationEventData.DIFF_OPERATION),
    @JsonSubTypes.Type(value = DiffEventDataV1.class, name = MutationEventData.IMPORT_OPERATION),
    @JsonSubTypes.Type(value = DiffEventDataV1.class, name = MutationEventData.RECONCILE_OPERATION),
    @JsonSubTypes.Type(value = MutationEventDataV1.class, name = MutationEventData.DELETE_OPERATION),
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
public class MutationEventDataV1 implements MutationEventData {
    public static final String SCHEMA = "/wikibase/rdf/update_stream/1.0.0";

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
    public MutationEventDataV1(
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
