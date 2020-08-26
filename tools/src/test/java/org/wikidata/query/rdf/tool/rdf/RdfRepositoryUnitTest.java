package org.wikidata.query.rdf.tool.rdf;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.test.Randomizer;
import org.wikidata.query.rdf.test.StatementHelper;
import org.wikidata.query.rdf.test.StatementHelper.StatementBuilder;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetricsResponseHandler;

import com.google.common.collect.ImmutableList;

/**
 * Test RdfRepository class.
 */
public class RdfRepositoryUnitTest {

    @Rule
    public final Randomizer randomizer = new Randomizer();

    private final UrisScheme uris = UrisSchemeFactory.getURISystem();

    @Test
    public void batchUpdate() {
        RdfClient mockClient = mock(RdfClient.class);
        // 1.5M size means ~4k statements or 250K statement size max
        long maxPostSize = 1572864L;
        CollectedUpdateMetrics collectedUpdateMetrics = new CollectedUpdateMetrics();
        collectedUpdateMetrics.setMutationCount(1);
        collectedUpdateMetrics.merge(MultiSyncStep.INSERT_NEW_DATA, UpdateMetrics.builder().build());
        when(mockClient.update(any(String.class), any(UpdateMetricsResponseHandler.class))).thenReturn(collectedUpdateMetrics);

        RdfRepository repo = new RdfRepository(uris, mockClient, maxPostSize);

        // 6000 statements - should go over the limit
        Change change1 = new Change("Q1", 1, Instant.EPOCH, 1);
        StatementBuilder sb = new StatementBuilder("Q1");
        for (int i = 0; i < 6000; i++) {
            sb.withPredicateObject(RDFS.LABEL, new LiteralImpl("some item " + i));
        }
        change1.setStatements(sb.build());
        // One statement with 300K data - should go over the limit
        Change change2 = new Change("Q2", 1, Instant.EPOCH, 1);
        List<Statement> statements2 = new StatementBuilder("Q2")
                .withPredicateObject(RDFS.LABEL, new LiteralImpl(randomizer.randomAsciiOfLength(300 * 1024)))
                .build();
        change2.setStatements(statements2);
        // Just one statement - this will be separated anyway
        Change change3 = new Change("Q3", 1, Instant.EPOCH, 1);
        List<Statement> statements3 = new StatementBuilder("Q3")
                .withPredicateObject(RDFS.LABEL, new LiteralImpl("third item"))
                .build();
        change3.setStatements(statements3);

        List<Change> changes = ImmutableList.of(change1, change2, change3);

        int count = repo.syncFromChanges(changes, false).getMutationCount();
        assertThat(count).isEqualTo(3);
        // We should get 3 calls to update
        verify(mockClient, times(3)).update(any(), any());
    }

    @Test
    public void testExtractValuesToCleanup() {
        List<Statement> statements = asList(
                StatementHelper.statement("val:quantity_to_keep", RDF.TYPE, Ontology.Quantity.TYPE),
                StatementHelper.statement("val:date_to_keep", RDF.TYPE, Ontology.Time.TYPE),
                StatementHelper.statement("val:coord_to_keep", RDF.TYPE, Ontology.Geo.TYPE),
                StatementHelper.statement("val:new_quantity", RDF.TYPE, Ontology.Quantity.TYPE),
                StatementHelper.statement("val:new_date", RDF.TYPE, Ontology.Time.TYPE),
                StatementHelper.statement("val:new_coord", RDF.TYPE, Ontology.Geo.TYPE)
        );
        Set<String> existingValues = new HashSet<>(asList("val:maybe_orphan_1", "val:maybe_orphan_2",
                "val:quantity_to_keep", "val:date_to_keep", "val:coord_to_keep"));
        assertThat(RdfRepository.extractValuesToCleanup(existingValues, statements))
                .containsExactlyInAnyOrder("val:maybe_orphan_1", "val:maybe_orphan_2");
    }

    @Test
    public void testExtractReferencesToCleanup() {
        List<Statement> statements = asList(
                StatementHelper.statement("ref:to_keep", RDF.TYPE, Ontology.REFERENCE),
                StatementHelper.statement("ref:new", RDF.TYPE, Ontology.REFERENCE)
        );
        Set<String> existingRefs = new HashSet<>(asList("ref:maybe_orphan_1", "ref:maybe_orphan_2", "ref:to_keep"));
        assertThat(RdfRepository.extractReferencesToCleanup(existingRefs, statements)).containsExactlyInAnyOrder("ref:maybe_orphan_1", "ref:maybe_orphan_2");
    }
}
