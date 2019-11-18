package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.test.Randomizer;
import org.wikidata.query.rdf.test.StatementHelper;
import org.wikidata.query.rdf.test.StatementHelper.StatementBuilder;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.rdf.RdfRepository.UpdateMode;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

/**
 * Test RdfRepository class.
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class RdfRepositoryUnitTest {

    @Rule
    public final Randomizer randomizer = new Randomizer();

    private abstract static class RdfEnv {
        static final UrisScheme uris = UrisSchemeFactory.getURISystem();
        // 1.5M size means ~4k statements or 250K statement size max
        static final long MAX_POST_SIZE = 1572864L;
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
        abstract void verifyUpdates(int count);
        abstract RdfRepository getRepo();
    }

    private static class NonMerging extends RdfEnv {
        private final RdfClient client;
        private final RdfRepository repo;
        @SuppressWarnings("unused") // Called by reflection
        NonMerging() {
            super();
            client = mock(RdfClient.class);
            when(client.update(any(String.class))).thenReturn(1);
            repo = new RdfRepository(uris, client, MAX_POST_SIZE, UpdateMode.NON_MERGING);
        }
        @Override
        RdfRepository getRepo() {
            return repo;
        }
        @Override
        void verifyUpdates(int count) {
            verify(client, times(count)).update(any());
        }
    }

    private static class Merging extends RdfEnv {
        private final RdfClient client;
        private final RdfRepository repo;
        @SuppressWarnings("unused") // Called by reflection
        Merging() {
            super();
            client = mock(RdfClient.class);
            when(client.mergingUpdate(any(Collection.class), any(Collection.class), any(Collection.class))).thenReturn(1);
            repo = new RdfRepository(uris, client, MAX_POST_SIZE, UpdateMode.MERGING);
        }
        public void verifyUpdates(int count) {
            verify(client, times(count)).mergingUpdate(any(Collection.class), any(Collection.class), any(Collection.class));
        }
        @Override
        public RdfRepository getRepo() {
            return repo;
        }
    }

    @Parameters(name = "check {0}")
    public static Collection<Class<? extends RdfEnv>> data() {
        return Arrays.asList(NonMerging.class, Merging.class);
    }

    private final RdfEnv rdfEnv;
    public RdfRepositoryUnitTest(Class<? extends RdfEnv> rdfEnv) throws InstantiationException, IllegalAccessException {
        this.rdfEnv = rdfEnv.newInstance();
    }

    // Creates a change with single generic statement
    private static Change createChange(String name, String label) {
        List<Statement> statements = new StatementBuilder(name)
                .withPredicateObject(RDFS.LABEL, new LiteralImpl(label))
                .build();
        return createChange(name, label, statements);
    }

    // Creates a change with provided statements
    private static Change createChange(String name, String label, List<Statement> statements) {
        Change change = new Change(name, 1, Instant.EPOCH, 1);
        change.setStatements(statements);
        return change;
    }

    private static List<Statement> createManyStatements(String name) {
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < 6000; i++) {
            StatementHelper.statement(statements, name, RDFS.LABEL, new LiteralImpl("some item " + i));
        }
        return statements;
    }

    private static final Change MANY_STATEMENTS = createChange("Q1", "many statements", createManyStatements("Q1"));
    private static final Change SMALL_STATEMENT = createChange("Q3", "small statement");
    private static final Change SMALL_STATEMENT2 = createChange("Q4", "another small statement");
    private final Change LARGE_STATEMENT = createChange("Q2", randomizer.randomAsciiOfLength(300 * 1024));

    @Test
    public void testOverflowOnCount() {
        // 6000 statements - should go over the limit
        // followed by just one statement - will be split in 2 batches as the first change overflows the batch
        List<Change> changes = Arrays.asList(MANY_STATEMENTS, SMALL_STATEMENT);
        int count = rdfEnv.getRepo().syncFromChanges(changes, /* verifyResult */ false);
        assertThat(count).isEqualTo(2);
        rdfEnv.verifyUpdates(count);
    }
    @Test
    public void testOverflowOnSize() {
        // One statement with 300K data - should go over the limit
        // followed by just one statement - will be split in 2 batches as the first change overflows the batch
        List<Change> changes = Arrays.asList(LARGE_STATEMENT, SMALL_STATEMENT);
        int count = rdfEnv.getRepo().syncFromChanges(changes, /* verifyResult */ false);
        assertThat(count).isEqualTo(2);
        rdfEnv.verifyUpdates(count);
    }
    @Test
    public void testBatchUp() {
        // One statement followed by another one statement - should be sent as a single batch
        List<Change> changes = Arrays.asList(SMALL_STATEMENT, SMALL_STATEMENT2);
        int count = rdfEnv.getRepo().syncFromChanges(changes, /* verifyResult */ false);
        assertThat(count).isEqualTo(1);
        rdfEnv.verifyUpdates(count);
    }
}
