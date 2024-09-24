package org.wikidata.query.rdf.updater;

import java.io.Serializable;
import java.time.Instant;

import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class MutationEventDataFactory implements Serializable  {
    DiffBuilder diffBuilder;
    MutationBuilder mutationBuilder;

    public static MutationEventDataFactory v1() {
        return new MutationEventDataFactory(DiffEventDataV1::new, MutationEventDataV1::new);
    }

    public static MutationEventDataFactory v2() {
        return new MutationEventDataFactory(DiffEventDataV2::new, MutationEventDataV2::new);
    }

    @FunctionalInterface
    public interface DiffBuilder extends Serializable {
        DiffEventData buildDiff(EventsMeta eventsMeta, String entity, long revision,
                                Instant eventTime, int sequence, int seqMax, String operation,
                                RDFDataChunk added, RDFDataChunk deleted, RDFDataChunk linkedShared,
                                RDFDataChunk unlinkedShared);
    }

    @FunctionalInterface
    public interface MutationBuilder extends Serializable {
        MutationEventData buildMutation(EventsMeta eventsMeta, String entity, long revision,
                                        Instant eventTime, int sequence, int seqMax, String operation);
    }
}
