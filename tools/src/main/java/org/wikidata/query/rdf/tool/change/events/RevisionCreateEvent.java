package org.wikidata.query.rdf.tool.change.events;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

/**
 * Record for mediawiki.revision-create scheme.
 * See: https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/revision/create/latest
 */
public class RevisionCreateEvent extends EventWithMeta {
    private final long revision;
    private final Long parentRevision;
    private final String title;
    private final long namespace;

    @VisibleForTesting
    public RevisionCreateEvent(EventsMeta meta, long revision, String title, long namespace) {
        this(meta, "", revision, null, title, namespace);
    }

    @VisibleForTesting
    public RevisionCreateEvent(EventInfo eventInfo, long revision, String title, long namespace) {
        super(eventInfo);
        this.revision = revision;
        this.parentRevision = null;
        this.title = title;
        this.namespace = namespace;
    }

    @JsonCreator
    public RevisionCreateEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty(EventInfo.SCHEMA_FIELD) String schema,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("rev_parent_id") @Nullable Long parentRevision,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace
    ) {
        super(meta, schema);
        this.revision = revision;
        this.parentRevision = parentRevision;
        this.title = title;
        this.namespace = namespace;
    }

    @Override
    public long revision() {
        return revision;
    }

    @Nullable
    public Long parentRevision() {
        return parentRevision;
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
