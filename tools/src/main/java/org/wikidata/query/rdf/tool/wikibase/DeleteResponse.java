package org.wikidata.query.rdf.tool.wikibase;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DeleteResponse extends WikibaseBaseResponse {
    @JsonCreator
    public DeleteResponse(
            @Nullable @JsonProperty("error") Object error) {
        super(error);
    }
}
