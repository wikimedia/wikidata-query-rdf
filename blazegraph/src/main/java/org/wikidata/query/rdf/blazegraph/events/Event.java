package org.wikidata.query.rdf.blazegraph.events;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface Event {
    @JsonProperty("meta")
    EventMetadata getMetadata();
}
