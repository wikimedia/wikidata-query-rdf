package org.wikidata.query.rdf.tool.stream;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.time.Instant;

import javax.annotation.Nullable;

import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.ToString;
import lombok.Value;

@Value
@ToString(callSuper = true)
public class DiffEventData extends MutationEventData {
    @JsonProperty("rdf_added_data")
    @JsonInclude(NON_NULL)
    RDFDataChunk rdfAddedData;

    @JsonProperty("rdf_deleted_data")
    @JsonInclude(NON_NULL)
    RDFDataChunk rdfDeletedData;

    /**
     * Data that is *perhaps* used by other items and thus already present in the target store.
     */
    @JsonProperty("rdf_linked_shared_data")
    @JsonInclude(NON_NULL)
    RDFDataChunk rdfLinkedSharedData;

    /**
     * Data that is no longer referenced from this item but *perhaps* still used by other items in the target store.
     */
    @JsonProperty("rdf_unlinked_shared_data")
    @JsonInclude(NON_NULL)
    RDFDataChunk rdfUnlinkedSharedData;

    @JsonCreator
    public DiffEventData(
            @JsonProperty("meta") EventsMeta meta,
            @JsonProperty("entity") String entity,
            @JsonProperty("revision") long revision,
            @JsonProperty("event_time") Instant eventTime,
            @JsonProperty("sequence") int sequence,
            @JsonProperty("max_sequence") int maxSequence,
            @JsonProperty("operation") String operation,
            @JsonProperty("rdf_added_data") @Nullable RDFDataChunk rdfAddedData,
            @JsonProperty("rdf_deleted_data") @Nullable RDFDataChunk rdfDeletedData,
            @JsonProperty("rdf_linked_shared_data") @Nullable RDFDataChunk rdfLinkedSharedData,
            @JsonProperty("rdf_unlinked_shared_data") @Nullable RDFDataChunk rdfUnlinkedSharedData
    ) {
        // custom ctor still needed as lombok is unable to construct this parent call
        super(meta, entity, revision, eventTime, sequence, maxSequence, operation);
        this.rdfAddedData = rdfAddedData;
        this.rdfDeletedData = rdfDeletedData;
        this.rdfLinkedSharedData = rdfLinkedSharedData;
        this.rdfUnlinkedSharedData = rdfUnlinkedSharedData;
    }
}
