package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Events for mediawiki.recentchange topic.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/recentchange/1.yaml
 * NOTE: currently not used for polling.
 */
public class RecentChangeEvent extends EventWithMeta {
    private final RevisionOldNew revision;
    private final String title;
    private final long namespace;

    @JsonCreator
    public RecentChangeEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("revision") RevisionOldNew revision,
            @JsonProperty("title") String title,
            @JsonProperty("namespace") long namespace
    ) {
        super(meta);
        this.revision = revision;
        this.title = title;
        this.namespace = namespace;
    }

    @Override
    public long revision() {
        return revision.revNew;
    }

    @Override
    public String title() {
        return title;
    }

    @Override
    public long namespace() {
        return namespace;
    }

    public static class RevisionOldNew {
        // Ignoring "old" part since it has no use for our scenario
        private final long revNew;

        @JsonCreator
        public RevisionOldNew(
                @JsonProperty("new") long rewnew
        ) {
            this.revNew = rewnew;
        }
    }

}
