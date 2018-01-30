package org.wikidata.query.rdf.tool.change.events;

import java.util.Map;

import org.wikidata.query.rdf.tool.change.Change;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Record for mediawiki.page-properties-change scheme.
 * See: https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/page/properties-change/1.yaml
 */
@SuppressFBWarnings(value = "DMC_DUBIOUS_MAP_COLLECTION", justification = "added/removed are maps in JSON")
public class PropertiesChangeEvent extends EventWithMeta {
    private final String title;
    private final long namespace;
    /**
     * Added properties.
     */
    private final Map<String, String> added;
    /**
     * Removed properties.
     */
    private final Map<String, String> removed;

    @JsonCreator
    public PropertiesChangeEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("page_title") String title,
            @JsonProperty("page_namespace") long namespace,
            @JsonProperty("added_properties") Map<String, String> added,
            @JsonProperty("removed_properties") Map<String, String> removed
    ) {
        super(meta);
        this.title = title;
        this.namespace = namespace;
        this.added = added;
        this.removed = removed;
    }

    @Override
    public long revision() {
        // No revision since property change does not have its own revision.
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
    public boolean isRedundant() {
        if (removed != null && !removed.isEmpty()) {
            // Removals are never redundant
            return false;
        }
        if (added == null || added.isEmpty()) {
            return true;
        }
        // If we have additions but not removals, this means it's likely a new page.
        // In this case we will declare it redundant if it only sets wb- properties,
        // since those are already accounted for in the actual page content.
        return added.entrySet().stream().anyMatch(entry -> {
            return !entry.getKey().startsWith("wb-");
        });
    }
}
