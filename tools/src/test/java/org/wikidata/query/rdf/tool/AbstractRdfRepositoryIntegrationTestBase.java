package org.wikidata.query.rdf.tool;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Superclass of integration tests that an RDF repository and clear it between
 * test methods.
 */
@RunWith(RandomizedRunner.class)
public abstract class AbstractRdfRepositoryIntegrationTestBase extends RandomizedTest {
    protected final EntityData entityDataUris;
    protected final Entity entityUris;
    protected final RdfRepositoryForTesting rdfRepository;

    /**
     * Build the test against prod wikidata.
     */
    public AbstractRdfRepositoryIntegrationTestBase() {
        this(EntityData.WIKIDATA, Entity.WIKIDATA);
    }

    public AbstractRdfRepositoryIntegrationTestBase(EntityData entityDataUris, Entity entityUris) {
        this.entityDataUris = entityDataUris;
        this.entityUris = entityUris;
        try {
            rdfRepository = new RdfRepositoryForTesting(new URI("http://localhost:9999/bigdata/namespace/kb/sparql"),
                    entityUris);
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
            execute("update", RdfRepository.UPDATE_COUNT_RESPONSE, "CLEAR DEFAULT");
        }

        /**
         * Loads a uri into this rdf repository. Uses Blazegraph's update with
         * uri's feature.
         */
        public int loadUrl(String uri) {
            return execute("uri", RdfRepository.UPDATE_COUNT_RESPONSE, uri);
        }
    }
}
