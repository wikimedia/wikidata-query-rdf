package org.wikidata.query.rdf.tool.wikibase;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Value;

@Value
public class LatestRevisionResponse implements WikibaseResponse {
    Query query;
    WikibaseApiError error;

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
    }

    @Value
    public static class Page {
        Long pageid;
        String title;
        List<Revision> revisions;
        boolean missing;
    }

    @Value
    public static class Revision {
        long revid;
    }

}
