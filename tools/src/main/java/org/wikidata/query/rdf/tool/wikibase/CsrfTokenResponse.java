package org.wikidata.query.rdf.tool.wikibase;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CsrfTokenResponse extends WikibaseBaseResponse {

    private final Query query;

    @JsonCreator
    public CsrfTokenResponse(
            @Nullable @JsonProperty("error") Object error,
            @JsonProperty("query") Query query) {
        super(error);
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    public static class Query {
        private final Tokens tokens;

        @JsonCreator
        public Query(@JsonProperty("tokens") Tokens tokens) {
            this.tokens = tokens;
        }

        public Tokens getTokens() {
            return tokens;
        }
    }

    public static class Tokens {
        private final String csrfToken;

        @JsonCreator
        public Tokens(@JsonProperty("csrftoken") String csrfToken) {
            this.csrfToken = csrfToken;
        }

        public String getCsrfToken() {
            return csrfToken;
        }
    }
}
