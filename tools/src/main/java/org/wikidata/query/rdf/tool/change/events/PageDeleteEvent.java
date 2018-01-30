package org.wikidata.query.rdf.tool.change.events;

import org.wikidata.query.rdf.tool.change.Change;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PageDeleteEvent extends EventWithMeta {
    private final String title;
    private final long namespace;

    @JsonCreator
    public PageDeleteEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace
    ) {
        super(meta);
        this.title = title;
        this.namespace = namespace;
    }

    @Override
    public long revision() {
        // Do not output revision since we're deleting the page, so we do not have revision number.
        return Change.NO_REVISION;
    }

    @Override
    public String title() {
        return title;
    }

    @Override
    public long namespace() {
        return namespace;
    }

}
