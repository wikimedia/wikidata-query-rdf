package org.wikidata.query.rdf.tool.change.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ReconcileEvent implements EventPlatformEvent {
    private final EventInfo eventInfo;
    private final String entity;
    private final long revision;
    private final Action reconciliationAction;
    private final EventInfo originalEventInfo;

    @JsonCreator
    public ReconcileEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("$schema") String schema,
            @JsonProperty("entity") String entity,
            @JsonProperty("rev_id") long revision,
            @JsonProperty("reconciliation_action") Action reconciliationAction,
            @JsonProperty("original_event_info") EventInfo originalEventInfo
    ) {
        this.eventInfo = new EventInfo(meta, schema);
        this.entity = entity;
        this.revision = revision;
        this.reconciliationAction = reconciliationAction;
        this.originalEventInfo = originalEventInfo;
    }

    @Override
    public EventsMeta meta() {
        return eventInfo.meta();
    }

    @Override
    public String schema() {
        return eventInfo.schema();
    }

    public enum Action {
        CREATION,
        DELETION
    }
}
