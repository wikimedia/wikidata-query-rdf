package org.wikidata.query.rdf.tool.change.events;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata for event record.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/resource_change/1.yaml
 */
public class EventsMeta {
    private final Instant timestamp;
    private final String id;
    private final String domain;

    @JsonCreator
    public EventsMeta(
            // Assumes the timestamp is in ISO 8601 format as this is what default Instant deserializer expects
            @JsonProperty("dt") Instant timestamp,
            @JsonProperty("id") String id,
            @JsonProperty("domain") String domain
    ) {
        this.timestamp = timestamp;
        this.id = id;
        this.domain = domain;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public String id() {
        return id;
    }

    public String domain() {
        return domain;
    }
}
