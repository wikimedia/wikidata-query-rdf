package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PageDeleteEvent extends EventWithMeta {
    private final long revision;
    private final String title;
    private final long namespace;

    @JsonCreator
    public PageDeleteEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace
    ) {
        super(meta);
        this.revision = revision;
        this.title = title;
        this.namespace = namespace;
    }

    @Override
    public long revision() {
        return revision;
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
