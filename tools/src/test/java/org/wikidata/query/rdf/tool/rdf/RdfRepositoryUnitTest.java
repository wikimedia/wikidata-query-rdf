package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.test.Randomizer;
import org.wikidata.query.rdf.test.StatementHelper.StatementBuilder;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

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
        when(mockClient.update(any(String.class))).thenReturn(1);

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

        int count = repo.syncFromChanges(changes, false);
        assertThat(count).isEqualTo(3);
        // We should get 3 calls to update
        verify(mockClient, times(3)).update(any());
    }

}
