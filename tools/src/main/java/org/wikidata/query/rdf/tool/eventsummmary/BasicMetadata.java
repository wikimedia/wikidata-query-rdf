package org.wikidata.query.rdf.tool.eventsummmary;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class BasicMetadata {
    private final String id;
    private final Instant dt;
}
