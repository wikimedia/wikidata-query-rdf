package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Record for mediawiki.page-undelete scheme.
 * See: https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/undelete/latest
 */
public class PageUndeleteEvent extends EventWithMeta {
    private final long pageId;
    private final long revision;
    private final String title;
    private final long namespace;

    @JsonCreator
    public PageUndeleteEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty(EventInfo.SCHEMA_FIELD) String schema,
            @JsonProperty("page_id") long pageId,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace
    ) {
        super(meta, schema);
        this.pageId = pageId;
        this.revision = revision;
        this.title = title;
        this.namespace = namespace;
    }

    public long pageId() {
        return pageId;
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
