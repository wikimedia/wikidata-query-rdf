package org.wikidata.query.rdf.tool;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.rdf.UpdateBuilder;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Superclass of integration tests that an RDF repository and clear it between
 * test methods.
 */
@RunWith(RandomizedRunner.class)
public abstract class AbstractRdfRepositoryIntegrationTestBase extends RandomizedTest {
    protected final RdfRepositoryForTesting rdfRepository;

    public AbstractRdfRepositoryIntegrationTestBase() {
        try {
            rdfRepository = new RdfRepositoryForTesting(new URI("http://localhost:9999/bigdata/namespace/kb/sparql"),
                    Entity.WIKIDATA);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void clear() {
        rdfRepository.clear();
    }

    /**
     * RdfRepository extension used for testing. We don't want to anyone to
     * accidentally use clear() so we don't put it in the repository.
     */
    public static class RdfRepositoryForTesting extends RdfRepository {
        public RdfRepositoryForTesting(URI uri, Entity entityUris) {
            super(uri, entityUris);
        }

        /**
         * Clear's the whole repository.
         */
        public void clear() {
            UpdateBuilder b = new UpdateBuilder();
            b.where("?s", "?p", "?o");
            b.delete("?s", "?p", "?o");
            execute("update", RdfRepository.IGNORE_RESPONSE, b.toString());
        }
    }
}
