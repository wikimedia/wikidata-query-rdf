package org.wikidata.query.rdf.updater;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MutationEventDataGenerator {
    private final RDFChunkSerializer rdfChunkSerializer;
    private final String mimeType;
    private final int softMaxRdfSize;
    private final MutationEventDataFactory mutationEventDataFactory;

    public List<MutationEventData> fullImportEvent(Supplier<EventsMeta> meta, String entity, long revision, Instant eventTime,
                                                     List<Statement> statements, List<Statement> linkedValuesAndRefs) {
        return createChunks(meta, entity, revision, eventTime, MutationEventData.IMPORT_OPERATION, statements,
                emptyList(), linkedValuesAndRefs, emptyList());
    }

    public List<MutationEventData> reconcile(Supplier<EventsMeta> meta, String entity, long revision, Instant eventTime,
                                               List<Statement> statements) {
        return createChunks(meta, entity, revision, eventTime, MutationEventData.RECONCILE_OPERATION, statements,
                emptyList(), emptyList(), emptyList());
    }

    public List<MutationEventData> diffEvent(Supplier<EventsMeta> meta, String entity, long revision, Instant eventTime,
                                               List<Statement> added, List<Statement> deleted,
                                               List<Statement> linkedValuesAndRefs, List<Statement> unlinkedValuesAndRefs) {
        return createChunks(meta, entity, revision, eventTime, MutationEventData.DIFF_OPERATION, added, deleted,
                linkedValuesAndRefs, unlinkedValuesAndRefs);
    }

    public List<MutationEventData> deleteEvent(Supplier<EventsMeta> meta, String entity, long revision, Instant eventTime) {
        MutationEventData deleteEvent = mutationEventDataFactory
                .getMutationBuilder()
                .buildMutation(meta.get(), entity, revision, eventTime, 0, 1, MutationEventData.DELETE_OPERATION);
        return singletonList(deleteEvent);
    }

    private List<MutationEventData> createChunks(Supplier<EventsMeta> meta, String entity, long revision, Instant eventTime,
                                                   String operation, List<Statement> added, List<Statement> deleted,
                                                   List<Statement> linkedValuesAndRefs,
                                                   List<Statement> unlinkedValuesAndRefs) {


        EnumMap<RDFKind, List<RDFDataChunk>> chunksPerType = new EnumMap<>(RDFKind.class);
        chunksPerType.put(RDFKind.Add, rdfChunkSerializer.serializeAsChunks(added, mimeType, softMaxRdfSize));
        chunksPerType.put(RDFKind.Del, rdfChunkSerializer.serializeAsChunks(deleted, mimeType, softMaxRdfSize));
        chunksPerType.put(RDFKind.LinkedShared, rdfChunkSerializer.serializeAsChunks(linkedValuesAndRefs, mimeType, softMaxRdfSize));
        chunksPerType.put(RDFKind.UnlinkedShared, rdfChunkSerializer.serializeAsChunks(unlinkedValuesAndRefs, mimeType, softMaxRdfSize));

        List<EnumMap<RDFKind, RDFDataChunk>> collected = new ArrayList<>(chunksPerType.values().stream().mapToInt(List::size).sum());
        Arrays.stream(RDFKind.values()).forEach(k -> mergeOrAppend(chunksPerType.get(k), collected, k));
        List<MutationEventData> events = new ArrayList<>(collected.size());
        int seqMax = collected.size();
        for (int i = 0; i < seqMax; i++) {
            EnumMap<RDFKind, RDFDataChunk> chunks = collected.get(i);
            events.add(mutationEventDataFactory.getDiffBuilder().buildDiff(meta.get(), entity, revision, eventTime, i, seqMax, operation,
                    chunks.get(RDFKind.Add), chunks.get(RDFKind.Del), chunks.get(RDFKind.LinkedShared), chunks.get(RDFKind.UnlinkedShared)));
        }
        return events;
    }

    /**
     * Tries to group chunks present in chunksToMergeOrAdd in with group of chunks present in the collect map for key kind.
     * A new group is created using if no mergeable element is found.
     * The goal is to merge as much as possible having the smallest number of elements possible in collect
     */
    private void mergeOrAppend(List<RDFDataChunk> chunksToMergeOrAdd, List<EnumMap<RDFKind, RDFDataChunk>> collect, RDFKind kind) {
        ToIntFunction<EnumMap<RDFKind, RDFDataChunk>> currentSize = r -> r.values().stream()
                .mapToInt(e -> e.getData().length())
                .sum();
        for (RDFDataChunk chunkToMergeOrAdd : chunksToMergeOrAdd) {
            Optional<EnumMap<RDFKind, RDFDataChunk>> mergeable = collect.stream()
                    .filter(group -> group.get(kind) == null)
                    .filter(group -> currentSize.applyAsInt(group) < softMaxRdfSize)
                    .findFirst();
            if (mergeable.isPresent()) {
                // We could stop early here and append all remaining elements
                // The way we chunk individual blocks in RDFChunkSerDeser::serializeAsChunks can only
                // produce one mergeable chunk
                mergeable.get().put(kind, chunkToMergeOrAdd);
            } else {
                EnumMap<RDFKind, RDFDataChunk> group = new EnumMap<>(RDFKind.class);
                group.put(kind, chunkToMergeOrAdd);
                collect.add(group);
            }
        }
    }

    private enum RDFKind {
        Add,
        Del,
        LinkedShared,
        UnlinkedShared,
    }
}
