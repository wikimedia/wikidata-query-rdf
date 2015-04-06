package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.allOf;
import static org.wikidata.query.rdf.tool.Matchers.binds;
import static org.wikidata.query.rdf.tool.StatementHelper.siteLink;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.tool.AbstractRdfRepositoryIntegrationTestBase;

import com.google.common.collect.ImmutableList;

/**
 * Tests RdfRepository against a live RDF repository.
 */
public class RdfRepositoryIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {
    @Test
    public void newSiteLink() throws QueryEvaluationException {
        rdfRepository.sync("Q23", siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washington", "en"));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s <http://schema.org/about> ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washington"),//
                binds("o", "Q23")));
        assertFalse(r.hasNext());
    }

    @Test
    public void moveSiteLink() throws QueryEvaluationException {
        newSiteLink();
        rdfRepository.sync("Q23", siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washingmoved", "en"));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s <http://schema.org/about> ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washingmoved"),//
                binds("o", "Q23")));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabel() throws QueryEvaluationException {
        rdfRepository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "en"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
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
        rdfRepository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washingmoved", "en"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George Washingmoved", "en"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabelWithQuotes() throws QueryEvaluationException {
        rdfRepository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George \"Cherry Tree\" Washington", "en"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"),//
                binds("p", RDFS.LABEL),//
                binds("o", new LiteralImpl("George \"Cherry Tree\" Washington", "en"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void statementWithBackslash() throws QueryEvaluationException {
        rdfRepository.sync("Q42", ImmutableList.of(//
                statement("Q42", "P396", new LiteralImpl("IT\\ICCU\\RAVV\\034417"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q42"),//
                binds("p", "P396"),//
                binds("o", new LiteralImpl("IT\\ICCU\\RAVV\\034417"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabelLanguage() throws QueryEvaluationException {
        newLabel();
        rdfRepository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "en")),//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "de"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o} ORDER BY ?o");
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
        assertFalse(rdfRepository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionFalseIfTooEarly() {
        syncJustVersion("Q23", 1);
        assertFalse(rdfRepository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionTrueIfMatch() {
        syncJustVersion("Q23", 10);
        assertTrue(rdfRepository.hasRevision("Q23", 10));
    }

    @Test
    public void hasRevisionTrueIfAfter() {
        syncJustVersion("Q23", 10);
        assertTrue(rdfRepository.hasRevision("Q23", 9));
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
        rdfRepository.sync("Q80", statements);
        rdfRepository.sync("Q80", statements);
        TupleQueryResult r = rdfRepository
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
        rdfRepository.sync("Q80", statements);
        rdfRepository.sync("Q80", statements);
        TupleQueryResult r = rdfRepository
                .query("PREFIX entity: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?p) as ?sc) WHERE {entity:Q80 ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(1000))));
        assertFalse(r.hasNext());
    }

    @Test
    public void delete() throws QueryEvaluationException {
        newSiteLink();
        rdfRepository.sync("Q23", Collections.<Statement> emptyList());
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertFalse(r.hasNext());
    }

    private void syncJustVersion(String entityId, int version) {
        Statement statement = statement(entityId, SchemaDotOrg.VERSION,
                new IntegerLiteralImpl(new BigInteger(Integer.toString(version))));
        rdfRepository.sync(entityId, ImmutableList.of(statement));
    }
}
