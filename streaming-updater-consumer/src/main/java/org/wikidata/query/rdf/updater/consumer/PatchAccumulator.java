package org.wikidata.query.rdf.updater.consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.wikidata.query.rdf.updater.MutationEventData.DELETE_OPERATION;
import static org.wikidata.query.rdf.updater.MutationEventData.DIFF_OPERATION;
import static org.wikidata.query.rdf.updater.MutationEventData.IMPORT_OPERATION;
import static org.wikidata.query.rdf.updater.MutationEventData.RECONCILE_OPERATION;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;
import org.wikidata.query.rdf.tool.rdf.Patch;
import org.wikidata.query.rdf.tool.rdf.SiteLinksReclassification;
import org.wikidata.query.rdf.updater.DiffEventData;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;
import org.wikidata.query.rdf.updater.RDFDataChunk;

import com.google.common.collect.Sets;

import lombok.Getter;

/**
 * Accumulates a collection of {@link MutationEventData} that can then be transformed into {@link ConsumerPatch} which
 * is then applied to a triple store.
 *
 * <p>The main principle is to collect {@link MutationEventData} and apply various optimizations to compress/drop
 * unnecessary mutations.
 *
 * <p>Order in which the data is collected is very important as the compression is done on the fly.
 * <p>Import and diff operations can be collected without restriction (except size considerations)
 * <p>Delete and Reconciliation operations might drop all the data collected, collecting subsequent operations on the same entity are
 * not possible and the patch must be materialized and applied
 *
 * <p>The impact of the resulting patch can be estimated using {@link #weight()} which will try to estimate the impact
 * (number of mutations) of the patch when applied to the store.
 *
 * <p>{@link #canAccumulate(MutationEventData)} must always be checked prior collecting more data, an exception will be
 * thrown otherwise.
 */
@Getter
@NotThreadSafe
public class PatchAccumulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(PatchAccumulator.class);
    private final Set<String> allEntitiesToDelete = new HashSet<>();
    private int totalAccumulated;
    private int invalidDuplicates;
    private final RDFChunkDeserializer deser;
    private static final int TRIPLES_PER_DELETE = 100;
    private final Map<Statement, String> allAddedMap = new HashMap<>();
    private final Map<Statement, String> allRemovedMap = new HashMap<>();
    private final Map<Statement, Set<String>> linkedSharedMap = new HashMap<>();
    private final Map<Statement, String> unlinkedSharedMap = new HashMap<>();
    private final Map<String, Collection<Statement>> reconciliations = new HashMap<>();

    public PatchAccumulator(RDFChunkDeserializer deser) {
        this.deser = deser;
    }

    public int weight() {
        return getNumberOfTriples() + (allEntitiesToDelete.size() * TRIPLES_PER_DELETE);
    }

    public int getNumberOfTriples() {
        return allAddedMap.size() + allRemovedMap.size() + linkedSharedMap.size() + unlinkedSharedMap.size() +
                reconciliations.values().stream().mapToInt(Collection::size).sum();
    }

    public int getNumberOfDeletedEntities() {
        return allEntitiesToDelete.size();
    }

    private void accumulate(String entityId, Collection<Statement> added, Collection<Statement> removed,
                            Collection<Statement> linkedSharedElts, Collection<Statement> unlinkedSharedElts) {
        totalAccumulated += added.size() + removed.size() + linkedSharedElts.size() + unlinkedSharedElts.size();

        Map<Statement, String> newlyAdded = added.stream()
                .collect(toMap(identity(), ei -> entityId, (e1, e2) -> entityId));
        countInvalidStatements(newlyAdded, allAddedMap);
        Map<Statement, String> newlyRemoved = removed.stream()
                .collect(toMap(identity(), ei -> entityId, (e1, e2) -> entityId));
        countInvalidStatements(newlyRemoved, allRemovedMap);
        Map<Statement, String> newlyLinkedSharedElts = linkedSharedElts.stream()
                .collect(toMap(identity(), ei -> entityId, (e1, e2) -> entityId));
        Map<Statement, String> newlyUnlinkedSharedElts = unlinkedSharedElts.stream()
                .collect(toMap(identity(), ei -> entityId, (e1, e2) -> entityId));
        removeIntersection(allAddedMap, newlyRemoved);
        removeIntersection(newlyAdded, allRemovedMap);
        removeIntersection(linkedSharedMap, newlyUnlinkedSharedElts);
        removeIntersection(unlinkedSharedMap, newlyLinkedSharedElts);
        newlyLinkedSharedElts.forEach((statement, entityIDMap) -> linkedSharedMap.computeIfAbsent(
                statement, k -> new HashSet<>()).add(entityIDMap));
        allAddedMap.putAll(newlyAdded);
        allRemovedMap.putAll(newlyRemoved);
        unlinkedSharedMap.putAll(newlyUnlinkedSharedElts);
    }

    private void countInvalidStatements(Map<Statement, String> newItemsMap, Map<Statement, String> allItemsMap) {
        newItemsMap.forEach((statement, entityId) -> {
            String currentEntityId = allItemsMap.get(statement);
            if (currentEntityId != null && !currentEntityId.equals(entityId)) {
                // NOTE: we should fail here and not allow this data
                // this is currently not possible because of https://phabricator.wikimedia.org/T317530
                // Once the root cause is fixed we might consider strengthening this again.
                LOGGER.warn("Trying to add/delete the same triple [{}] " +
                        "for a different entities: [{}] and [{}]", statement, currentEntityId, entityId);
                invalidDuplicates++;
            }
        });
    }

    /**
     * Whether or not the patch given as argument can be accumulated in this accumulator.
     */
    public boolean canAccumulate(MutationEventData data) {
        switch (data.getOperation()) {
            case DELETE_OPERATION:
                // We could possibly skip the reconciliation?
                return !reconciliations.containsKey(data.getEntity());
            case RECONCILE_OPERATION:
                return !allEntitiesToDelete.contains(data.getEntity());
            case IMPORT_OPERATION:
            case DIFF_OPERATION:
                return !allEntitiesToDelete.contains(data.getEntity()) && !reconciliations.containsKey(data.getEntity());
            default:
                throw new UnsupportedOperationException("Unsupported operation [" + data.getOperation() + "]");
        }
    }

    private void removeIntersection(Map<Statement, ?> map1, Map<Statement, String> map2) {
        Set<Statement> intersection = new HashSet<>(Sets.intersection(map1.keySet(), map2.keySet()));
        map1.keySet().removeAll(intersection);
        map2.keySet().removeAll(intersection);
    }

    public ConsumerPatch asPatch() {
        return new ConsumerPatch(unmodifiableList(new ArrayList<>(allAddedMap.keySet())),
                unmodifiableList(new ArrayList<>(linkedSharedMap.keySet())),
                unmodifiableList(new ArrayList<>(allRemovedMap.keySet())),
                unmodifiableList(new ArrayList<>(unlinkedSharedMap.keySet())),
                unmodifiableList(new ArrayList<>(allEntitiesToDelete)),
                unmodifiableMap(reconciliations));
    }

    public void accumulate(List<MutationEventData> sequence) {
        checkPositionIndex(0, sequence.size(), "Received empty sequence");
        MutationEventData head = sequence.get(0);
        checkArgument(canAccumulate(head), "Cannot accumulate data for entity: " + head.getEntity());

        switch (head.getOperation()) {
            case DELETE_OPERATION:
                checkArgument(sequence.size() == 1, "Inconsistent delete mutation (" + sequence.size() + " chunks)");
                accumulateDelete(head);
                break;
            case IMPORT_OPERATION:
            case DIFF_OPERATION:
                checkArgument(head instanceof DiffEventData, "Unsupported MutationEventData of type " + head.getOperation());
                accumulateDiff(sequence);
                break;
            case RECONCILE_OPERATION:
                checkArgument(head instanceof DiffEventData, "Unsupported MutationEventData of type " + head.getOperation());
                accumulateReconciliation(sequence);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation [" + head.getOperation() + "]");
        }
    }

    private void accumulateReconciliation(List<MutationEventData> sequence) {
        checkPositionIndex(0, sequence.size(), "Received empty sequence");
        MutationEventData head = sequence.get(0);
        Optional<MutationEventData> inconsistentBlock = sequence.stream().filter(m -> {
            if (!head.getEntity().equals(m.getEntity())) {
                return true;
            } else if (!m.getMeta().requestId().equals(head.getMeta().requestId())) {
                return true;
            } else return !head.getOperation().equals(m.getOperation());
        }).findFirst();
        if (inconsistentBlock.isPresent()) {
            throw new IllegalArgumentException("Inconsistent sequence of events: " + inconsistentBlock.get() + " does not belong to " + head);
        }
        List<Statement> allStmts = sequence.stream()
                .map(DiffEventData.class::cast)
                .map(DiffEventData::getRdfAddedData)
                .flatMap(c -> deserChunk(c).stream())
                .collect(toList());

        reconciliations.put(head.getEntity(), allStmts);
        // Drop patch data from this entity since we are reconciling it we will reset all that anyways
        removeDataFromEntity(head.getEntity());
        totalAccumulated += allStmts.size();
    }

    private void accumulateDiff(List<MutationEventData> sequence) {
        MutationEventData head = sequence.get(0);
        List<Statement> added = new ArrayList<>();
        List<Statement> removed = new ArrayList<>();
        List<Statement> linkedShared = new ArrayList<>();
        List<Statement> unlinkedShared = new ArrayList<>();

        for (MutationEventData data: sequence) {
            if (!head.getClass().equals(data.getClass())) {
                throw new IllegalArgumentException("Inconsistent chunks provided, head class " +
                        head.getClass() + " does not match " + data.getClass());
            }
            if (!head.getMeta().requestId().equals(data.getMeta().requestId())) {
                throw new IllegalArgumentException("Inconsistent chunks provided, head requestId " +
                        head.getMeta().requestId() + " does not match " + data.getMeta().requestId());
            }
            DiffEventData diff = (DiffEventData) data;
            if (diff.getRdfAddedData() != null) {
                added.addAll(deserChunk(diff.getRdfAddedData()));
            }
            if (diff.getRdfDeletedData() != null) {
                removed.addAll(deserChunk(diff.getRdfDeletedData()));
            }
            if (diff.getRdfLinkedSharedData() != null) {
                linkedShared.addAll(deserChunk(diff.getRdfLinkedSharedData()));
            }
            if (diff.getRdfUnlinkedSharedData() != null) {
                unlinkedShared.addAll(deserChunk(diff.getRdfUnlinkedSharedData()));
            }
        }

        Patch patch = SiteLinksReclassification.reclassify(new Patch(added, linkedShared, removed, unlinkedShared));
        accumulate(head.getEntity(), patch.getAdded(), patch.getRemoved(), patch.getLinkedSharedElements(), patch.getUnlinkedSharedElements());
    }

    private void accumulateDelete(MutationEventData delete) {
        totalAccumulated += TRIPLES_PER_DELETE;
        String entityId = delete.getEntity();
        allEntitiesToDelete.add(entityId);
        removeDataFromEntity(entityId);
    }

    private void removeDataFromEntity(String entityId) {
        allAddedMap.entrySet().removeIf(entry -> entry.getValue().equals(entityId));
        allRemovedMap.entrySet().removeIf(entry -> entry.getValue().equals(entityId));
        linkedSharedMap.entrySet().removeIf(entry -> entry.getValue().contains(entityId) && entry.getValue().size() <= 1);
    }

    public void accumulate(MutationEventData value) {
        accumulate(singletonList(value));
    }

    private List<Statement> deserChunk(RDFDataChunk chunk) {
        return deser.deser(chunk, "unused");
    }
}
