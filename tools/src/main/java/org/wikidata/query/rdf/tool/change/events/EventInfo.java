package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class EventInfo {
    public static final String SCHEMA_FIELD = "$schema";
    EventsMeta meta;
    String schema;

    @JsonCreator
    public EventInfo(@JsonProperty("meta") EventsMeta meta, @JsonProperty(SCHEMA_FIELD) String schema) {
        this.meta = meta;
        this.schema = schema;
    }
}
