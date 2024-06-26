package org.wikidata.query.rdf.tool.change.events;

import java.time.Instant;

import javax.annotation.Nullable;

import org.wikidata.query.rdf.tool.EntityId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ReconcileEvent implements EventPlatformEvent {
    public static final String SCHEMA = "/rdf_streaming_updater/reconcile/1.0.0";
    @JsonIgnore
    private final EventInfo eventInfo;
    @JsonProperty("item")
    private final EntityId item;
    @JsonProperty("revision_id")
    private final long revision;
    @JsonProperty("reconciliation_source")
    private final String reconciliationSource;
    @JsonProperty("reconciliation_action")
    private final Action reconciliationAction;
    @JsonProperty("original_event_info")
    private final EventInfo originalEventInfo;

    @JsonCreator
    public ReconcileEvent(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("$schema") String schema,
            @JsonProperty("item") EntityId item,
            @JsonProperty("revision_id") long revision,
            @JsonProperty("reconciliation_source") String reconciliationSource,
            @JsonProperty("reconciliation_action") Action reconciliationAction,
            @Nullable @JsonProperty("original_event_info") EventInfo originalEventInfo
    ) {
        this.eventInfo = new EventInfo(meta, schema);
        this.item = item;
        this.revision = revision;
        this.reconciliationAction = reconciliationAction;
        this.reconciliationSource = reconciliationSource;
        this.originalEventInfo = originalEventInfo;
    }

    @Override
    @JsonProperty("meta")
    public EventsMeta meta() {
        return eventInfo.meta();
    }

    @Override
    @JsonProperty("$schema")
    public String schema() {
        return eventInfo.schema();
    }

    public enum Action {
        CREATION,
        DELETION
    }

    /** Repurpose this event with a new event time. */
    public ReconcileEvent overrideEventTime(Instant eventTime) {
        return new ReconcileEvent(
                new EventsMeta(eventTime, meta().id(), meta().domain(), meta().stream(), meta().requestId()),
                schema(), item, revision, reconciliationSource, reconciliationAction, originalEventInfo);
    }
}
