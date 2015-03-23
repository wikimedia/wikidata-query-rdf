package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.tool.Matchers.binds;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.google.common.collect.ImmutableList;

/**
 * Tests RdfRepository against a live RDF repository.
 */
public class RdfRepositoryIntegrationTest {
    private final RdfRepositoryForTesting repository;

    public RdfRepositoryIntegrationTest() throws URISyntaxException {
        repository = new RdfRepositoryForTesting(new URI("http://localhost:9999/bigdata/namespace/kb/sparql"),
                Entity.WIKIDATA);
    }

    @Before
    public void clear() {
        repository.clear();
    }

    @Test
    public void newSiteLink() throws QueryEvaluationException {
        repository.sync("Q23", ImmutableList.of(//
                statement("http://en.wikipedia.org/wiki/George_Washington", SchemaDotOrg.ABOUT, "Q23")));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washington"),//
                binds("p", SchemaDotOrg.ABOUT),//
                binds("o", "Q23")));
        assertFalse(r.hasNext());
    }

    @Test
    public void moveSiteLink() throws QueryEvaluationException {
        newSiteLink();
        repository.sync("Q23", ImmutableList.of(//
                statement("http://en.wikipedia.org/wiki/George_Washingmoved", SchemaDotOrg.ABOUT, "Q23")));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washingmoved"),//
                binds("p", SchemaDotOrg.ABOUT),//
                binds("o", "Q23")));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabel() throws QueryEvaluationException {
        repository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "en"))));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George Washington", "en"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void changedLabel() throws QueryEvaluationException {
        newLabel();
        repository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washingmoved", "en"))));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George Washingmoved", "en"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabelLanguage() throws QueryEvaluationException {
        newLabel();
        repository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "en")),//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "de"))));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o} ORDER BY ?o");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George Washington", "de"))));
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George Washington", "en"))));
        assertFalse(r.hasNext());
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
            execute("update", null, b.toString());
        }
    }
}
