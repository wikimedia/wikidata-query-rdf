package org.wikidata.query.rdf.blazegraph.events;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventPerformer {
    private final String userText;

    public EventPerformer(String userText) {
        this.userText = userText;
    }

    @JsonProperty("user_text")
    public String getUserText() {
        return userText;
    }
}
