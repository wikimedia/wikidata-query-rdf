package org.wikidata.query.rdf.tool.wikibase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Continue {
    private final String rcContinue;
    private final String aContinue;

    @JsonCreator
    public Continue(
            @JsonProperty("rccontinue") String rcContinue,
            @JsonProperty("continue") String aContinue) {
        this.rcContinue = rcContinue;
        this.aContinue = aContinue;
    }

    public String getRcContinue() {
        return rcContinue;
    }

    public String getContinue() {
        return aContinue;
    }

    @Override
    public String toString() {
        return "Continue{rccontinue=" + rcContinue + ",continue=" + aContinue + "}";
    }
}
