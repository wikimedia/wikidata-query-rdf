package org.wikidata.query.rdf.tool.wikibase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DeleteResponse extends WikibaseResponse {
    @JsonCreator
    public DeleteResponse(
            @JsonProperty("error") Object error) {
        super(error);
    }
}
