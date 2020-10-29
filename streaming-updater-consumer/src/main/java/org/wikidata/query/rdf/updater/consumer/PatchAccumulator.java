package org.wikidata.query.rdf.updater.consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;
import org.wikidata.query.rdf.updater.DiffEventData;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;

import com.google.common.collect.Sets;

import lombok.Getter;

@Getter
@NotThreadSafe
public class PatchAccumulator {
    private final Set<String> allEntitiesToDelete = new HashSet<>();
    private int totalAccumulated;
    private final RDFChunkDeserializer deser;
    private static final int TRIPLES_PER_DELETE = 100;
    private final Map<Statement, String> allAddedMap = new HashMap<>();
    private final Map<Statement, String> allRemovedMap = new HashMap<>();
    private final Map<Statement, Set<String>> linkedSharedMap = new HashMap<>();
    private final Map<Statement, String> unlinkedSharedMap = new HashMap<>();

    public PatchAccumulator(RDFChunkDeserializer deser) {
        this.deser = deser;
    }

    public int weight() {
        int approximateNumOfDeletedTriples = allEntitiesToDelete.size() * TRIPLES_PER_DELETE;
        return allAddedMap.size() + allRemovedMap.size() + linkedSharedMap.size() + unlinkedSharedMap.size() + approximateNumOfDeletedTriples;
    }

    private void accumulate(String entityId, Collection<Statement> added, Collection<Statement> removed,
                            Collection<Statement> linkedSharedElts, Collection<Statement> unlinkedSharedElts) {
        totalAccumulated += added.size() + removed.size() + linkedSharedElts.size() + unlinkedSharedElts.size();
        Map<Statement, String> newlyAdded = added.stream().collect(toMap(identity(), ei -> entityId));
        findInvalidStatements(newlyAdded, allAddedMap);
        Map<Statement, String> newlyRemoved = removed.stream().collect(toMap(identity(), ei -> entityId));
        findInvalidStatements(newlyRemoved, allRemovedMap);
        Map<Statement, String> newlyLinkedSharedElts = linkedSharedElts.stream().collect(toMap(identity(), ei -> entityId));
        Map<Statement, String> newlyUnlinkedSharedElts = unlinkedSharedElts.stream().collect(toMap(identity(), ei -> entityId));
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

    private void findInvalidStatements(Map<Statement, String> newItemsMap, Map<Statement, String> allItemsMap) {
        newItemsMap.forEach((statement, entityId) -> {
            String currentEntityId = allItemsMap.get(statement);
            if (currentEntityId != null && !currentEntityId.equals(entityId)) {
                throw new IllegalArgumentException("Cannot add/delete the same triple [" + statement + "] " +
                        "for a different entities: [" + currentEntityId + "] and [" + entityId + "]");
            }
        });
    }

    /**
     * Whether or not the patch given as argument can be accumulated in this accumulator.
     */
    public boolean canAccumulate(MutationEventData data) {
        if (!data.getOperation().equals(DiffEventData.DELETE_OPERATION))
            return !allEntitiesToDelete.contains(data.getEntity());
        return true;
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
                unmodifiableList(new ArrayList<>(allEntitiesToDelete)));
    }

    public void accumulate(MutationEventData value) {
        if (!canAccumulate(value)) {
            throw new IllegalArgumentException("Cannot accumulate data for entity: " + value.getEntity());
        }
        if (value instanceof DiffEventData) {
            DiffEventData val = (DiffEventData) value;
            accumulate(value.getEntity(), val.getRdfAddedData() != null ? deser.deser(val.getRdfAddedData(), "unused") : emptyList(),
                    val.getRdfDeletedData() != null ? deser.deser(val.getRdfDeletedData(), "unused") : emptyList(),
                    val.getRdfLinkedSharedData() != null ? deser.deser(val.getRdfLinkedSharedData(), "unused") : emptyList(),
                    val.getRdfUnlinkedSharedData() != null ? deser.deser(val.getRdfUnlinkedSharedData(), "unused") : emptyList());
        } else if (MutationEventData.DELETE_OPERATION.equals(value.getOperation())) {
            totalAccumulated += TRIPLES_PER_DELETE;
            String entityId = value.getEntity();
            allEntitiesToDelete.add(entityId);
            allAddedMap.entrySet().removeIf(entry -> entry.getValue().equals(entityId));
            allRemovedMap.entrySet().removeIf(entry -> entry.getValue().equals(entityId));
            linkedSharedMap.entrySet().removeIf(entry -> entry.getValue().contains(entityId) && entry.getValue().size() <= 1);
        } else {
            throw new IllegalArgumentException("Unsupported MutationEventData of type " + value.getOperation());
        }
    }

}
