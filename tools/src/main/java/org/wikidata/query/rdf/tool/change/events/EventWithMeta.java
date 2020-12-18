package org.wikidata.query.rdf.tool.change.events;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base class for event that has metadata.
 */
public abstract class EventWithMeta implements ChangeEvent {
    private final EventInfo eventInfo;

    @JsonCreator
    EventWithMeta(@JsonProperty("meta") EventsMeta meta, @JsonProperty(EventInfo.SCHEMA_FIELD) String schema) {
        eventInfo = new EventInfo(meta, schema);
    }

    EventWithMeta(EventInfo info) {
        this.eventInfo = info;
    }

    public EventInfo eventInfo() {
        return eventInfo;
    }

    @Override
    public Instant timestamp() {
        return eventInfo.meta().timestamp();
    }

    public String id() {
        return eventInfo.meta().id();
    }

    @Override
    public String domain() {
        return eventInfo.meta().domain();
    }
}
