package org.wikidata.query.rdf.tool.wikibase;

import java.util.List;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchResponse extends WikibaseBaseResponse {

    private final List<SearchResult> search;

    @JsonCreator
    public SearchResponse(
            @Nullable @JsonProperty("error") Object error,
            @JsonProperty("search") List<SearchResult> search) {
        super(error);
        this.search = search;
    }

    public List<SearchResult> getSearch() {
        return search;
    }


    public static class SearchResult {
        private final String id;

        @JsonCreator
        public SearchResult(
                @JsonProperty("id") String id
        ) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
