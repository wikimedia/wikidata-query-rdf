package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.tool.Matchers.binds;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.RDF;
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
    public void newLabelWithQuotes() throws QueryEvaluationException {
        repository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George \"Cherry Tree\" Washington", "en"))));
        TupleQueryResult r = repository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George \"Cherry Tree\" Washington", "en"))));
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

    @Test
    public void hasRevisionFalseIfNotPresent() {
        assertFalse(repository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionFalseIfTooEarly() {
        syncJustVersion("Q23", 1);
        assertFalse(repository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionTrueIfMatch() {
        syncJustVersion("Q23", 10);
        assertTrue(repository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionTrueIfAfter() {
        syncJustVersion("Q23", 10);
        assertTrue(repository.hasRevision("Q23", 9));
    }

    /**
     * Updating items with lots of sitelinks shouldn't be painfully slow.
     */
    @Test(timeout = 10000)
    public void lotsOfSiteLinks() throws QueryEvaluationException {
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            String link = String.format(Locale.ROOT, "http://%s.example.com/wiki/tbl", i);
            statements.add(statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE));
            statements.add(statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(Integer.toString(i))));
            statements.add(statement(link, SchemaDotOrg.ABOUT, "Q80"));
        }
        repository.sync("Q80", statements);
        repository.sync("Q80", statements);
        TupleQueryResult r = repository
                .query("PREFIX entity: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?s) as ?sc) WHERE {?s ?p entity:Q80}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(800))));
        assertFalse(r.hasNext());
    }

    /**
     * Updating items with lots of statements shouldn't be painfully slow.
     */
    @Test(timeout = 10000)
    public void lotsOfStatements() throws QueryEvaluationException {
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            statements.add(statement("Q80", "P" + i, new IntegerLiteralImpl(BigInteger.valueOf(i))));
        }
        repository.sync("Q80", statements);
        repository.sync("Q80", statements);
        TupleQueryResult r = repository
                .query("PREFIX entity: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?p) as ?sc) WHERE {entity:Q80 ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(1000))));
        assertFalse(r.hasNext());
    }

    private void syncJustVersion(String entityId, int version) {
        Statement statement = statement(entityId, SchemaDotOrg.VERSION,
                new IntegerLiteralImpl(new BigInteger(Integer.toString(version))));
        repository.sync(entityId, ImmutableList.of(statement));
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
