package org.wikidata.query.rdf.updater.consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikidata.query.rdf.test.StatementHelper.statement;
import static org.wikidata.query.rdf.test.StatementHelper.uri;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataFactory;
import org.wikidata.query.rdf.updater.MutationEventDataGenerator;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;
import org.wikidata.query.rdf.updater.RDFChunkSerializer;


public class UpdatePatchAccumulatorUnitTest {
    private final RDFChunkSerializer serializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
    private final RDFChunkDeserializer deserializer = new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance()));
    private final MutationEventDataGenerator eventGenerator = new MutationEventDataGenerator(serializer, RDFFormat.TURTLE.getDefaultMIMEType(),
            10, MutationEventDataFactory.v2());

    @Test
    public void test_accumulate_should_optimize_and_ignore_added_and_then_removed_triples() {
        PatchAccumulator accum = new PatchAccumulator(deserializer);
        accumulateDiff(accum, "Q1",
                singletonList(stmt("uri:added")),
                singletonList(stmt("uri:deleted")),
                singletonList(stmt("uri:linked")),
                singletonList(stmt("uri:unlinked"))
        );

        assertThat(accum.getTotalAccumulated()).isEqualTo(4);
        assertThat(accum.weight()).isEqualTo(4);
        assertThat(accum.getAllAddedMap().keySet()).containsOnly(stmt("uri:added"));
        assertThat(accum.getAllRemovedMap().keySet()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getLinkedSharedMap().keySet()).containsOnly(stmt("uri:linked"));
        assertThat(accum.getUnlinkedSharedMap().keySet()).containsOnly(stmt("uri:unlinked"));

        accumulateDiff(accum, "Q1",
                singletonList(stmt("uri:A1")),
                emptyList(),
                singletonList(stmt("uri:L1")),
                emptyList()
        );
        accumulateDelete(accum, "entity123");

        assertThat(accum.getTotalAccumulated()).isEqualTo(106);
        assertThat(accum.weight()).isEqualTo(106);
        assertThat(accum.getAllAddedMap().keySet()).containsOnly(stmt("uri:added"), stmt("uri:A1"));
        assertThat(accum.getAllRemovedMap().keySet()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getLinkedSharedMap().keySet()).containsOnly(stmt("uri:linked"), stmt("uri:L1"));
        assertThat(accum.getUnlinkedSharedMap().keySet()).containsOnly(stmt("uri:unlinked"));
        assertThat(accum.getAllEntitiesToDelete()).containsOnly("entity123");

        accumulateDiff(accum, "Q1",
                singletonList(stmt("uri:A2")),
                singletonList(stmt("uri:A1")),
                asList(stmt("uri:L2"), stmt("uri:shared")),
                singletonList(stmt("uri:L1"))
        );

        assertThat(accum.getTotalAccumulated()).isEqualTo(111);
        assertThat(accum.weight()).isEqualTo(107);
        assertThat(accum.getAllAddedMap().keySet()).containsOnly(stmt("uri:added"), stmt("uri:A2"));
        assertThat(accum.getAllRemovedMap().keySet()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getLinkedSharedMap().keySet()).containsOnly(stmt("uri:linked"), stmt("uri:L2"), stmt("uri:shared"));
        assertThat(accum.getUnlinkedSharedMap().keySet()).containsOnly(stmt("uri:unlinked"));

        accumulateDiff(accum, "Q1",
                singletonList(stmt("uri:A1")),
                singletonList(stmt("uri:A2")),
                singletonList(stmt("uri:L1")),
                singletonList(stmt("uri:L2"))
        );

        assertThat(accum.getTotalAccumulated()).isEqualTo(115);
        assertThat(accum.weight()).isEqualTo(107);
        assertThat(accum.getAllAddedMap().keySet()).containsOnly(stmt("uri:added"), stmt("uri:A1"));
        assertThat(accum.getAllRemovedMap().keySet()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getLinkedSharedMap().keySet()).containsOnly(stmt("uri:linked"), stmt("uri:L1"), stmt("uri:shared"));
        assertThat(accum.getUnlinkedSharedMap().keySet()).containsOnly(stmt("uri:unlinked"));
    }

    @Test
    public void test_cannot_accumulate_similar_triples_for_unrelated_entities() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);
        accumulateDiff(accumulator, "Q1",
                singletonList(stmt("uri:added")),
                singletonList(stmt("uri:removed")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        accumulateDiff(accumulator, "Q2",
                singletonList(stmt("uri:added")),
                emptyList(),
                emptyList(),
                emptyList());
        assertThat(accumulator.getInvalidDuplicates()).isEqualTo(1);

        accumulateDiff(accumulator, "Q2",
                emptyList(),
                singletonList(stmt("uri:removed")),
                emptyList(),
                emptyList());
        assertThat(accumulator.getInvalidDuplicates()).isEqualTo(2);

        ConsumerPatch expectedPatch = accumulator.asPatch();
        accumulateDiff(accumulator, "Q2",
                emptyList(),
                emptyList(),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        assertThat(accumulator.asPatch())
                .withFailMessage("Accumulating same shared statements for different entities should result in the same patch")
                .isEqualTo(expectedPatch);

        assertThat(accumulator.getInvalidDuplicates()).isEqualTo(2);
    }

    @Test
    public void test_add_then_remove_same_entity_should_create_a_path_without_triples_for_this_entity() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        accumulateDiff(accumulator, "UNRELATED",
                singletonList(stmt("uri:added-unrelated-entity")),
                singletonList(stmt("uri:deleted-unrelated-entity")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        accumulateDiff(accumulator, "Q1",
                singletonList(stmt("uri:added-Q1")),
                singletonList(stmt("uri:deleted-Q1")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        accumulateDelete(accumulator, "Q1");
        ConsumerPatch expected = new ConsumerPatch(
                singletonList(stmt("uri:added-unrelated-entity")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:deleted-unrelated-entity")),
                singletonList(stmt("uri:unlinked-shared")),
                singletonList("Q1"),
                Collections.emptyMap()
        );
        assertThat(accumulator.asPatch())
                .withFailMessage("Deleting an entity should create a patch without any triples for this entity")
                .isEqualTo(expected);
    }

    @Test
    public void test_remove_then_add_or_reconcile_should_not_be_supported_in_the_same_patch() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        accumulateDiff(accumulator, "UNRELATED",
                singletonList(stmt("uri:added-unrelated-entity")),
                singletonList(stmt("uri:deleted-unrelated-entity")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        MutationEventData deleteEvent = eventGenerator.deleteEvent(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH).get(0);
        assertThat(accumulator.canAccumulate(deleteEvent)).isTrue();
        accumulator.accumulate(deleteEvent);
        assertThat(accumulator.canAccumulate(deleteEvent))
                .withFailMessage("Deleting the same entity twice should accepted and be a no-op")
                .isTrue();

        MutationEventData insertEvent = eventGenerator.fullImportEvent(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH,
                singletonList(stmt("uri:added-for-Q1")),
                singletonList(stmt("uri:linked-shared"))
        ).get(0);

        assertThat(accumulator.canAccumulate(insertEvent))
                .withFailMessage("Re-inserting the same entity after a delete in the same accumulator should not be supported")
                .isFalse();

        assertThatThrownBy(() -> accumulateDiff(accumulator, "Q1",
                singletonList(stmt("uri:added-for-Q1")),
                singletonList(stmt("uri:linked-shared")),
                emptyList(),
                emptyList()
        ), "Caller should call canAccumulate and not try to 'force-add' triples for an entity that is already accumulated as 'deleted'")
                .isInstanceOf(IllegalArgumentException.class);

        MutationEventData reconcile = eventGenerator.reconcile(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH,
                singletonList(stmt("uri:added-for-Q1"))).get(0);

        assertThat(accumulator.canAccumulate(reconcile))
                .withFailMessage("Reconciling the same entity after a delete in the same accumulator should not be supported")
                .isFalse();
    }

    @Test
    public void test_add_then_remove_should_try_to_prune_removed_entity_shared_triples() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        accumulateDiff(accumulator, "UNRELATED",
                singletonList(stmt("uri:added-unrelated-entity")),
                singletonList(stmt("uri:deleted-unrelated-entity")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        accumulateDiff(accumulator, "Q1",
                singletonList(stmt("uri:added-Q1")),
                singletonList(stmt("uri:deleted-Q1")),
                asList(stmt("uri:linked-shared"), stmt("uri:")),
                singletonList(stmt("uri:unlinked-shared"))
        );
        accumulateDiff(accumulator, "Q1",
                emptyList(),
                emptyList(),
                singletonList(stmt("uri:")),
                emptyList()
        );
        accumulateDelete(accumulator, "Q1");
        ConsumerPatch expected = new ConsumerPatch(
                singletonList(stmt("uri:added-unrelated-entity")),
                singletonList(stmt("uri:linked-shared")),
                singletonList(stmt("uri:deleted-unrelated-entity")),
                singletonList(stmt("uri:unlinked-shared")),
                singletonList("Q1"),
                Collections.emptyMap()
        );
        assertThat(accumulator.asPatch())
                .withFailMessage("Deleting an entity should create a patch without any triples for this entity even shared ones")
                .isEqualTo(expected);
    }

    @Test
    public void test_invalid_duplicates_are_counted() {
        MutationEventDataGenerator eventGenerator = new MutationEventDataGenerator(
                serializer, RDFFormat.TURTLE.getDefaultMIMEType(), 300, MutationEventDataFactory.v2());

        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        List<MutationEventData> events = eventGenerator.diffEvent(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH,
                singletonList(stmt("uri:added-Q1")),
                singletonList(stmt("uri:deleted-Q1")),
                asList(stmt("uri:linked-shared"), stmt("uri:")),
                singletonList(stmt("uri:unlinked-shared")));
        events.forEach(accumulator::accumulate);

        List<MutationEventData> events2 = eventGenerator.diffEvent(metaGenerator("Q2"), "Q2", 1, Instant.EPOCH,
                asList(stmt("uri:added-Q2"), stmt("uri:added-Q1")),
                singletonList(stmt("uri:deleted-Q1")),
                asList(stmt("uri:linked-shared"), stmt("uri:")),
                singletonList(stmt("uri:unlinked-shared")));

        events2.forEach(accumulator::accumulate);
        assertThat(accumulator.getInvalidDuplicates()).isEqualTo(2);
    }

    @Test
    public void test_add_then_remove_must_not_prune_unrelated_shared_triples() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);
        accumulateDiff(accumulator, "Q1",
                singletonList(stmt("uri:added-Q1")),
                singletonList(stmt("uri:deleted-Q1")),
                asList(stmt("uri:linked-shared"), stmt("uri:linked-shared-only-for-Q1")),
                asList(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-only-for-Q1"))
        );
        accumulateDiff(accumulator, "Q2",
                singletonList(stmt("uri:added-Q2")),
                singletonList(stmt("uri:deleted-Q2")),
                asList(stmt("uri:linked-shared"), stmt("uri:linked-shared-only-for-Q2")),
                asList(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-only-for-Q2"))
        );
        accumulateDelete(accumulator, "Q1");
        ConsumerPatch actual = accumulator.asPatch();
        assertThat(actual.getLinkedSharedElements())
                .containsExactlyInAnyOrder(stmt("uri:linked-shared"), stmt("uri:linked-shared-only-for-Q2"));
        assertThat(actual.getUnlinkedSharedElements())
                .contains(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-only-for-Q2"));
        // It's OK to keep/not keep uri:unlinked-shared-only-for-Q1 here
    }

    @Test
    public void test_duplicated_values_can_be_accumulated() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        MutationEventDataGenerator bigChunkEventGenerator = new MutationEventDataGenerator(
                serializer, RDFFormat.TURTLE.getDefaultMIMEType(), Integer.MAX_VALUE, MutationEventDataFactory.v2());
        accumulateDiff(accumulator, "Q1",
                asList(stmt("uri:added-1"), stmt("uri:added-1")),
                asList(stmt("uri:removed-1"), stmt("uri:removed-1")),
                asList(stmt("uri:linked-shared"), stmt("uri:linked-shared")),
                asList(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared")),
                bigChunkEventGenerator
        );
        ConsumerPatch actual = accumulator.asPatch();

        assertThat(actual.getAdded())
                .containsExactlyInAnyOrder(stmt("uri:added-1"));
        assertThat(actual.getRemoved())
                .contains(stmt("uri:removed-1"));
        assertThat(actual.getLinkedSharedElements())
                .containsExactlyInAnyOrder(stmt("uri:linked-shared"));
        assertThat(actual.getUnlinkedSharedElements())
                .contains(stmt("uri:unlinked-shared"));
    }

    @Test
    public void test_reconcile_operation_can_be_accumulated() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);

        MutationEventDataGenerator bigChunkEventGenerator = new MutationEventDataGenerator(
                serializer, RDFFormat.TURTLE.getDefaultMIMEType(), Integer.MAX_VALUE, MutationEventDataFactory.v2());
        accumulateDiff(accumulator, "Q1",
                asList(stmt("uri:added-1"), stmt("uri:added-1")),
                asList(stmt("uri:removed-1"), stmt("uri:removed-1")),
                asList(stmt("uri:linked-shared"), stmt("uri:linked-shared-1")),
                asList(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-1")),
                bigChunkEventGenerator
        );

        accumulateDiff(accumulator, "Q2",
                asList(stmt("uri:added-2"), stmt("uri:added-2")),
                asList(stmt("uri:removed-2"), stmt("uri:removed-2")),
                asList(stmt("uri:linked-shared"), stmt("uri:linked-shared-2")),
                asList(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-2")),
                bigChunkEventGenerator
        );

        accumulateReconciliation(accumulator, "Q1", singletonList(stmt("uri:reconciled-1")));
        ConsumerPatch consumerPatch = accumulator.asPatch();
        assertThat(consumerPatch.getAdded())
                .containsExactlyInAnyOrder(stmt("uri:added-2"));
        assertThat(consumerPatch.getRemoved())
                .containsExactlyInAnyOrder(stmt("uri:removed-2"));
        assertThat(consumerPatch.getLinkedSharedElements())
                .containsExactlyInAnyOrder(stmt("uri:linked-shared"), stmt("uri:linked-shared-2"));
        assertThat(consumerPatch.getUnlinkedSharedElements())
                .contains(stmt("uri:unlinked-shared"), stmt("uri:unlinked-shared-2"));
        assertThat(consumerPatch.getReconciliations()).containsOnlyKeys("Q1");
        assertThat(consumerPatch.getReconciliations()).containsValue(singletonList(stmt("uri:reconciled-1")));
    }

    @Test
    public void test_reconcile_operation_fails_with_unrelated_sequence() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);
        List<Statement> stmts = IntStream.range(0, 100).mapToObj(i -> stmt("uri:stmt-" + i)).collect(toList());
        List<MutationEventData> events = eventGenerator.reconcile(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH, stmts);
        accumulator.accumulate(events);
        List<MutationEventData> wrongBatch = new ArrayList<>(events);
        wrongBatch.addAll(eventGenerator.reconcile(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH, stmts));
        assertThatThrownBy(() -> accumulator.accumulate(wrongBatch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Inconsistent sequence of events:");
    }

    @Test
    public void test_patch_cannot_be_accumulated_with_reconciliation() {
        PatchAccumulator accumulator = new PatchAccumulator(deserializer);
        List<MutationEventData> events = eventGenerator.reconcile(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH,
                singletonList(stmt("uri:stmt")));
        accumulator.accumulate(events);
        List<MutationEventData> diff = eventGenerator.diffEvent(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH,
                singletonList(stmt("uri:added-1")), singletonList(stmt("uri:removed-1")), emptyList(), emptyList());
        assertThat(accumulator.canAccumulate(diff.get(0))).isFalse();
        List<MutationEventData> del = eventGenerator.deleteEvent(metaGenerator("Q1"), "Q1", 1, Instant.EPOCH);
        assertThat(accumulator.canAccumulate(del.get(0))).isFalse();
    }

    private void accumulateDiff(PatchAccumulator accumulator,
                                String entityId,
                                List<Statement> added,
                                List<Statement> removed,
                                List<Statement> linkedSharedElts,
                                List<Statement> unlinkedSharedElts) {
        accumulateDiff(accumulator, entityId, added, removed, linkedSharedElts, unlinkedSharedElts, eventGenerator);
    }

    private void accumulateDiff(PatchAccumulator accumulator,
                                String entityId,
                                List<Statement> added,
                                List<Statement> removed,
                                List<Statement> linkedSharedElts,
                                List<Statement> unlinkedSharedElts,
                                MutationEventDataGenerator generator
    ) {
        List<MutationEventData> events = generator.diffEvent(metaGenerator(entityId), entityId, 1, Instant.EPOCH,
                added, removed, linkedSharedElts, unlinkedSharedElts);
        accumulator.accumulate(events);
    }

    private void accumulateDelete(PatchAccumulator accumulator, String entityId) {
        List<MutationEventData> events = eventGenerator.deleteEvent(metaGenerator(entityId), entityId, 1, Instant.EPOCH);
        accumulator.accumulate(events);
    }

    private void accumulateReconciliation(PatchAccumulator accumulator, String entityId, List<Statement> stmts) {
        List<MutationEventData> events = eventGenerator.reconcile(metaGenerator(entityId), entityId, 1, Instant.EPOCH, stmts);
        accumulator.accumulate(events);
    }

    private Statement stmt(String s) {
        return statement(s, s, uri(s));
    }

    private Supplier<EventsMeta> metaGenerator(String entityId) {
        String requestId = UUID.randomUUID().toString();
        return () -> new EventsMeta(Instant.EPOCH, entityId, "", "", requestId);
    }
}
