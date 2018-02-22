package org.wikidata.query.rdf.tool.change.events;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base class for event that has metadata.
 */
public abstract class EventWithMeta implements ChangeEvent {
    /**
     * Metadata record.
     */
    private final EventsMeta meta;

    @JsonCreator
    EventWithMeta(@JsonProperty("meta") EventsMeta meta) {
        this.meta = meta;
    }

    public EventsMeta meta() {
        return meta;
    }

    @Override
    public Instant timestamp() {
        return meta.timestamp();
    }

    public String id() {
        return meta.id();
    }

    @Override
    public String domain() {
        return meta.domain();
    }

    @Override
    public boolean isRedundant() {
        // By default it's not redundant
        return false;
    }
}
