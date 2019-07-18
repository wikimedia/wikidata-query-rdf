package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

/**
 * Record for mediawiki.revision-create scheme.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/revision/create/2.yaml
 */
public class RevisionCreateEvent extends EventWithMeta implements EventWithChronology {
    private final long revision;
    private final String title;
    private final long namespace;
    private final String chronologyId;

    @JsonCreator
    public RevisionCreateEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace,
            @JsonProperty("chronology_id") String chronologyId
    ) {
        super(meta);
        this.revision = revision;
        this.title = title;
        this.namespace = namespace;
        this.chronologyId = Strings.emptyToNull(chronologyId);
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

    @Override
    public String chronologyId() {
        return chronologyId;
    }
}
