package org.wikidata.query.rdf.tool.change.events;

import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;



/**
 * Record for mediawiki.revision-create scheme.
 * See: https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/revision/create/latest
 */
public class RevisionCreateEvent extends EventWithMeta {
    private final long pageId;
    private final long revision;
    private final Long parentRevision;
    private final String title;
    private final long namespace;
    private final Map<String, RevisionSlot> revSlots;

    @VisibleForTesting
    public RevisionCreateEvent(EventsMeta meta,
                               long pageId,
                               long revision,
                               String title,
                               long namespace,
                               Map<String, RevisionSlot> revSlots) {
        this(meta, "", pageId, revision, null, title, namespace, revSlots);
    }

    @VisibleForTesting
    public RevisionCreateEvent(EventInfo eventInfo,
                               long pageId,
                               long revision,
                               String title,
                               long namespace,
                               Map<String, RevisionSlot> revSlots) {
        super(eventInfo);
        this.pageId = pageId;
        this.revision = revision;
        this.parentRevision = null;
        this.title = title;
        this.namespace = namespace;
        this.revSlots = revSlots;
    }

    @JsonCreator
    public RevisionCreateEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty(EventInfo.SCHEMA_FIELD) String schema,
            @JsonProperty("page_id") long pageId,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("rev_parent_id") @Nullable Long parentRevision,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace,
            @JsonProperty("rev_slots") Map<String, RevisionSlot> revSlots) {
        super(meta, schema);
        this.pageId = pageId;
        this.revision = revision;
        this.parentRevision = parentRevision;
        this.title = title;
        this.namespace = namespace;
        this.revSlots = revSlots;
    }

    public long pageId() {
        return pageId;
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

    public Map<String, RevisionSlot> revSlots() {
        return revSlots;
    }
}
