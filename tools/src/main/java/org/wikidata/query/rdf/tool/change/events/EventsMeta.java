package org.wikidata.query.rdf.tool.change.events;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Metadata for event record.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/resource_change/1.yaml
 */
@SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"}, justification = "Yup, Date is mutable, can't do much about it")
public class EventsMeta {
    private final Date timestamp;
    private final String id;
    private final String domain;

    public static final String INPUT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX";

    @JsonCreator
    public EventsMeta(
            @JsonProperty("dt") @JsonFormat(shape = STRING, pattern = INPUT_DATE_FORMAT) Date timestamp,
            @JsonProperty("id") String id,
            @JsonProperty("domain") String domain
    ) {
        this.timestamp = timestamp;
        this.id = id;
        this.domain = domain;
    }

    public Date timestamp() {
        return timestamp;
    }

    public String id() {
        return id;
    }

    public String domain() {
        return domain;
    }
}
