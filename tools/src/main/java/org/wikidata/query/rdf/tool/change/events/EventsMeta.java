package org.wikidata.query.rdf.tool.change.events;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata for event record.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/resource_change/1.yaml
 */
public class EventsMeta {
    private final Instant timestamp;
    private final String id;
    private final String domain;

    public static final String INPUT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX";

    @JsonCreator
    public EventsMeta(
            @JsonProperty("dt") @JsonFormat(shape = STRING, pattern = INPUT_DATE_FORMAT) Instant timestamp,
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
