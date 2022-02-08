package org.wikidata.query.rdf.tool.wikibase;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;

@Value
public class LatestRevisionResponse implements WikibaseResponse {
    Query query;
    WikibaseApiError error;

    /**
     * Explicit constructor with jackson annotations.
     *
     * TODO: drop this constructor once we are running a version of spark that embarks a newer version of jackson
     * supporting this pattern.
     */
    @JsonCreator
    public LatestRevisionResponse(@JsonProperty("query") Query query, @Nullable @JsonProperty("error") WikibaseApiError error) {
        this.query = query;
        this.error = error;
    }

    public Optional<Long> latestRevisionForPageid(long pageid) {
        return latestRevisionFor(Page::getPageid, id -> id == pageid);
    }

    public Optional<Long> latestRevisionForTitle(String title) {
        return latestRevisionFor(Page::getTitle, title::equals);
    }

    private <W> Optional<Long> latestRevisionFor(Function<Page, W> get, Predicate<W> matches) {
        if (error != null) {
            throw new IllegalStateException("This response has errors: " + error);
        }
        for (Page p: query.getPages()) {
            W w = get.apply(p);
            if (w == null) {
                throw new IllegalStateException("Use proper API params 'titles' not 'ids' for checking by title or page id");
            }
            if (matches.test(w)) {
                return p.isMissing() ? Optional.empty() : Optional.of(p.getRevisions().get(0).getRevid());
            }
        }
        throw new IllegalStateException("Revision not found");
    }

    @Value
    public static class Query {
        List<Page> pages;
        @JsonCreator
        public Query(@JsonProperty("pages") List<Page> pages) {
            this.pages = pages;
        }
    }

    @Value
    public static class Page {
        Long pageid;
        String title;
        List<Revision> revisions;
        boolean missing;

        @JsonCreator
        public Page(@JsonProperty("pageid") Long pageid,
                    @JsonProperty("title") String title,
                    @JsonProperty("revisions") List<Revision> revisions,
                    @JsonProperty("missing") boolean missing) {
            this.pageid = pageid;
            this.title = title;
            this.revisions = revisions;
            this.missing = missing;
        }
    }

    @Value
    public static class Revision {
        long revid;

        @JsonCreator
        public Revision(@JsonProperty("revid") long revid) {
            this.revid = revid;
        }
    }

}
