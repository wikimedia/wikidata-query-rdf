package org.wikidata.query.rdf.tool.change.events;

import org.wikidata.query.rdf.tool.change.Change;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

public class PageDeleteEvent extends EventWithMeta implements EventWithChronology {
    private final String title;
    private final long namespace;
    private final String chronologyId;

    @JsonCreator
    public PageDeleteEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace,
            @JsonProperty("chronology_id") String chronologyId
    ) {
        super(meta);
        this.title = title;
        this.namespace = namespace;
        this.chronologyId = Strings.emptyToNull(chronologyId);
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

    @Override
    public String chronologyId() {
        return chronologyId;
    }
}
