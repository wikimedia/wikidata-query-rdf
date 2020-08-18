package org.wikidata.query.rdf.tool.eventsummmary;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BasicQueryEvent {
    private final BasicMetadata meta;
}
