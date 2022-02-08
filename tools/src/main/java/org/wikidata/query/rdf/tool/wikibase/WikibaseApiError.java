package org.wikidata.query.rdf.tool.wikibase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;

@Value
public class WikibaseApiError {
    /**
     * Explicit constructor with jackson annotations.
     *
     * TODO: drop this constructor once we are running a version of spark that embarks a newer version of jackson
     * supporting this pattern.
     */
    @JsonCreator
    public WikibaseApiError(@JsonProperty("code") String code, @JsonProperty("info") String info) {
        this.code = code;
        this.info = info;
    }
    String code;
    String info;
}
