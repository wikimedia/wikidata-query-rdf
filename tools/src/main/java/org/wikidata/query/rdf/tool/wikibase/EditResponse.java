package org.wikidata.query.rdf.tool.wikibase;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EditResponse extends WikibaseBaseResponse {

    private final Entity entity;

    @JsonCreator
    public EditResponse(
            @Nullable @JsonProperty("error") Object error,
            @JsonProperty("entity") Entity entity) {
        super(error);
        this.entity = entity;
    }

    public Entity getEntity() {
        return entity;
    }

    public static class Entity {
        private final String id;

        @JsonCreator
        public Entity(@JsonProperty("id") String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
