package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.allOf;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.test.StatementHelper.siteLink;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;
import org.wikidata.query.rdf.test.StatementHelper.StatementBuilder;
import org.wikidata.query.rdf.tool.RdfRepositoryForTesting;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.ImmutableList;

/**
 * Tests RdfRepository against a live RDF repository.
 */
@RunWith(RandomizedRunner.class)
public class RdfRepositoryIntegrationTest extends RandomizedTest {
    private Set<String> cleanupList = new HashSet<>();

    /**
     * Wikibase uris to test with.
     */
    private final WikibaseUris uris = WikibaseUris.getURISystem();
    private final Munger munger = new Munger(uris);

    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq");

    @Before
    public void cleanList() {
        cleanupList = new HashSet<>();
    }

    @Test
    public void newSiteLink() throws QueryEvaluationException {
        rdfRepository.sync("Q23", siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washington", "en"));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s <http://schema.org/about> ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washington"), //
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
                binds("s", "http://en.wikipedia.org/wiki/George_Washingmoved"), //
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
                binds("s", "Q23"), //
                binds("p", RDFS.LABEL), //
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
                binds("s", "Q23"), //
                binds("p", RDFS.LABEL), //
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
                binds("s", "Q23"), //
                binds("p", RDFS.LABEL), //
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
                binds("s", "Q42"), //
                binds("p", "P396"), //
                binds("o", new LiteralImpl("IT\\ICCU\\RAVV\\034417"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void statementToEntityDoesntRemoveEntity() throws QueryEvaluationException {
        Statement link = statement("Q23", "P26", "Q191789");
        Statement onGeorge = statement("Q23", "P20", "Q494413");
        Statement onMartha = statement("Q191789", "P20", "Q731635");
        rdfRepository.sync("Q23", ImmutableList.of(link, onGeorge));
        rdfRepository.sync("Q191789", ImmutableList.of(onMartha));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P20 wd:Q494413 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P26 wd:Q191789 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q191789 p:P20 wd:Q731635 }"));

        rdfRepository.sync("Q23", ImmutableList.of(onGeorge));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P20 wd:Q494413 }"));
        assertFalse(rdfRepository.ask("ASK {wd:Q23 p:P26 wd:Q191789 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q191789 p:P20 wd:Q731635 }"));
    }

    @Test
    public void newLabelLanguage() throws QueryEvaluationException {
        newLabel();
        rdfRepository.sync("Q23", ImmutableList.of(//
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "en")), //
                statement("Q23", RDFS.LABEL, new LiteralImpl("George Washington", "de"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o} ORDER BY ?o");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"), //
                binds("p", RDFS.LABEL), //
                binds("o", new LiteralImpl("George Washington", "de"))));
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q23"), //
                binds("p", RDFS.LABEL), //
                binds("o", new LiteralImpl("George Washington", "en"))));
        assertFalse(r.hasNext());
    }

    @Test
    public void basicExpandedStatement() throws QueryEvaluationException {
        List<Statement> george = expandedStatement("ce976010-412f-637b-c687-9fd2d52dc140", "Q23", "P509", "Q356405",
                Ontology.NORMAL_RANK);
        rdfRepository.sync("Q23", george);
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P509 [ ps:P509 ?cause; wikibase:rank wikibase:NormalRank ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("cause", "Q356405"));
        assertFalse(r.hasNext());
    }

    @Test
    public void changedExpandedStatementValue() throws QueryEvaluationException {
        basicExpandedStatement();
        List<Statement> george = expandedStatement("ce976010-412f-637b-c687-9fd2d52dc140", "Q23", "P509", "Q3736439",
                Ontology.NORMAL_RANK);
        // Poor George Washington's cause of death is now duck
        rdfRepository.sync("Q23", george);
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P509 [ ps:P509 ?cause; wikibase:rank wikibase:NormalRank ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("cause", "Q3736439"));
        assertFalse(r.hasNext());
    }

    @Test
    public void changedExpandedStatementRank() throws QueryEvaluationException {
        basicExpandedStatement();
        List<Statement> george = expandedStatement("ce976010-412f-637b-c687-9fd2d52dc140", "Q23", "P509", "Q356405",
                Ontology.DEPRECATED_RANK);
        rdfRepository.sync("Q23", george);
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P509 [ ps:P509 ?cause; wikibase:rank wikibase:DeprecatedRank ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("cause", "Q356405"));
        assertFalse(r.hasNext());
    }

    @Test
    public void expandedStatementWithExpandedValue() throws QueryEvaluationException {
        String statementUri = uris.statement() + "someotheruuid";
        String valueUri = uris.value() + "someuuid";
        cleanupList.add(valueUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeCalendarValue(valueUri, "cat", "animals")
                .build();

        rdfRepository.sync("Q23", george);
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"cat\" ] ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeCalendarModel \"animals\" ] ] }"));
    }

    @Test
    public void expandedStatementWithExpandedValueChanged() throws QueryEvaluationException {
        expandedStatementWithExpandedValue();
        String statementUri = uris.statement() + "someotheruuid";
        String valueUri = uris.value() + "newuuid";
        cleanupList.add(valueUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeCalendarValue(valueUri, "dog", "animals")
                .build();

        rdfRepository.sync("Q23", george);
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"dog\" ] ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeCalendarModel \"animals\" ] ] }"));
        assertFalse(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"cat\" ] ] }"));
    }

    @Test
    public void referenceWithExpandedValue() throws QueryEvaluationException {
        String statementUri = uris.statement() + "someotheruuid";
        String referenceUri = uris.reference() + "yetanotheruri";
        String valueUri = uris.value() + "someuuid";
        cleanupList.add(valueUri);
        cleanupList.add(referenceUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withReferenceValue(referenceUri, "P509", valueUri)
                .withReference(referenceUri, "P143", "Q328")
                .withTimeCalendarValue(valueUri, "cat", "animals")
                .build();

        rdfRepository.sync("Q23", george);
        List<Statement> enwiki = new ArrayList<>();
        statement(enwiki, "Q328", "P509", "Q328");
        rdfRepository.sync("Q328", enwiki);
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ prv:P509 [ wikibase:timeValue \"cat\" ] ] ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ prv:P509 [ wikibase:timeCalendarModel \"animals\" ] ] ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ pr:P143 [ p:P509 wd:Q328 ] ] ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q328 p:P509 wd:Q328 }"));
    }

    @Test
    public void referenceWithExpandedValueChanged() throws QueryEvaluationException {
        referenceWithExpandedValue();
        String statementUri = uris.statement() + "someotheruuid";
        String referenceUri = uris.reference() + "andanotheruri";
        String valueUri = uris.value() + "someuuid2";
        cleanupList.add(valueUri);
        cleanupList.add(referenceUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withReferenceValue(referenceUri, "P509", valueUri)
                .withTimeCalendarValue(valueUri, "dog", "animals")
                .build();

        rdfRepository.sync("Q23", george, cleanupList);
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ prv:P509 [ wikibase:timeValue \"dog\" ] ] ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ prv:P509 [ wikibase:timeCalendarModel \"animals\" ] ] ] }"));
        assertFalse(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ prv:P509 [ wikibase:timeTime \"cat\" ] ] ] }"));
        // We've unlinked enwiki
        assertFalse(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ prov:wasDerivedFrom [ pr:P143 [ p:P509 wd:Q328 ] ] ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q328 p:P509 wd:Q328 }"));
    }

    @Test
    public void referencesOnExpandedStatements() throws QueryEvaluationException {
        String referenceUri = uris.reference() + "e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P854", "http://www.anb.org/articles/02/02-00332.html");
        cleanupList.add(referenceUri);
        rdfRepository.sync("Q23", george, cleanupList);
        TupleQueryResult r = rdfRepository.query(
                "SELECT * WHERE { wd:Q23 p:P19 [ ps:P19 ?placeOfBirth; prov:wasDerivedFrom [ ?provP ?provO ] ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("placeOfBirth", "Q494413"), //
                binds("provP", uris.property(PropertyType.REFERENCE) + "P854"), //
                binds("provO", "http://www.anb.org/articles/02/02-00332.html")));
        assertFalse(r.hasNext());
    }

    @Test
    public void referencesOnExpandedStatementsChangeValue() throws QueryEvaluationException {
        referencesOnExpandedStatements();
        String referenceUri = uris.reference() + "new-e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P854", "http://example.com");
        rdfRepository.sync("Q23", george, cleanupList);
        TupleQueryResult r = rdfRepository.query(
                "SELECT * WHERE { wd:Q23 p:P19 [ ps:P19 ?placeOfBirth; prov:wasDerivedFrom [ ?provP ?provO ] ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("placeOfBirth", "Q494413"), //
                binds("provP", uris.property(PropertyType.REFERENCE) + "P854"), //
                binds("provO", "http://example.com")));
        assertFalse(r.hasNext());
    }

    @Test
    public void referencesOnExpandedStatementsChangePredicate() throws QueryEvaluationException {
        referencesOnExpandedStatements();
        String referenceUri = uris.reference() + "new-e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P143", "http://www.anb.org/articles/02/02-00332.html");
        rdfRepository.sync("Q23", george, cleanupList);
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P19 [ ps:P19 ?placeOfBirth; prov:wasDerivedFrom [ ?provP ?provO ] ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("placeOfBirth", "Q494413"), //
                binds("provP", uris.property(PropertyType.REFERENCE) + "P143"), //
                binds("provO", "http://www.anb.org/articles/02/02-00332.html")));
        assertFalse(r.hasNext());
    }

    @Test
    public void sharedReference() throws QueryEvaluationException {
        // Load a shared reference
        String referenceUri = uris.reference() + "e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        Statement refDecl = statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P854",
                "http://www.anb.org/articles/02/02-00332.html");
        rdfRepository.sync("Q23", george);
        List<Statement> dummy = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q1234134", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        dummy.add(refDecl);
        rdfRepository.sync("Q1234134", dummy, cleanupList);

        // Now query and make sure you can find it
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P19 [ ps:P19 ?placeOfBirth; prov:wasDerivedFrom [ ?provP ?provO ] ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("placeOfBirth", "Q494413"), //
                binds("provP", uris.property(PropertyType.REFERENCE) + "P854"), //
                binds("provO", "http://www.anb.org/articles/02/02-00332.html")));
        assertFalse(r.hasNext());

        // Now remove the reference from just one place
        dummy = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q1234134", "P19", "Q494413",
                Ontology.NORMAL_RANK);
        rdfRepository.sync("Q1234134", dummy, cleanupList);

        // Now query and find the reference still there because its shared!
        r = rdfRepository.query("SELECT * WHERE { wd:Q23 p:P19 [ ps:P19 ?placeOfBirth; prov:wasDerivedFrom [ ?provP ?provO ] ] }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("placeOfBirth", "Q494413"), //
                binds("provP", uris.property(PropertyType.REFERENCE) + "P854"), //
                binds("provO", "http://www.anb.org/articles/02/02-00332.html")));
        assertFalse(r.hasNext());

        // Query one more way just to be sure it works 10000%
        r = rdfRepository.query("SELECT * WHERE { ?s ?p ?o . FILTER( STRSTARTS(STR(?s), \""
                        + uris.reference() + "\") ) . }");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("p", uris.property(PropertyType.REFERENCE) + "P854"), //
                binds("o", "http://www.anb.org/articles/02/02-00332.html")));
        assertFalse(r.hasNext());

        // Now remove it from its last place
        george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK);
        rdfRepository.sync("Q23", george, cleanupList);

        /*
         * Now query and find the reference now gone because it isn't used
         * anywhere.
         */
        r = rdfRepository.query("SELECT * WHERE { ?s ?p ?o . FILTER( STRSTARTS(STR(?s), \""
                        + uris.reference() + "\") ) . }");
        assertFalse(r.hasNext());
    }

    /**
     * Tests that if a reference is used multiple times on the same entity it
     * isn't cleared if its removed from the entity just once. That'd be wrong
     * because its still on the entity.
     */
    @Test
    public void sharedReferenceOnSameEntity() {
        String referenceUri = uris.reference() + "e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P854", "http://www.anb.org/articles/02/02-00332.html");
        List<Statement> georgeWithoutSecondReference = new ArrayList<>(george);
        String otherStatementUri = uris.statement() + "ASDFasdf";
        statement(george, "Q23", uris.property(PropertyType.CLAIM) + "P129", otherStatementUri);
        statement(george, otherStatementUri, uris.property(PropertyType.STATEMENT) + "P129", new LiteralImpl("cat"));
        statement(george, otherStatementUri, Provenance.WAS_DERIVED_FROM, referenceUri);
        rdfRepository.sync("Q23", george, cleanupList);
        assertTrue(rdfRepository.ask("ASK {wdref:e36b7373814a0b74caa84a5fc2b1e3297060ab0f pr:P854 ?o }"));

        rdfRepository.sync("Q23", georgeWithoutSecondReference, cleanupList);
        assertTrue(rdfRepository.ask("ASK {wdref:e36b7373814a0b74caa84a5fc2b1e3297060ab0f pr:P854 ?o }"));
    }

    // TODO values on shared references change when they change

    private List<Statement> expandedStatement(String statementId, String subject, String predicate, String value,
            String rank, String referenceUri) {
        List<Statement> statements = expandedStatement(statementId, subject, predicate, value, rank);
        String statementUri = statements.get(1).getSubject().stringValue();
        statement(statements, statementUri, Provenance.WAS_DERIVED_FROM, referenceUri);
        return statements;
    }

    private List<Statement> expandedStatement(String statementId, String subject, String predicate, String value,
            String rank) {
        List<Statement> statements = new ArrayList<>();
        String statementUri = uris.statement() + subject + "-" + statementId;
        statement(statements, subject, predicate, statementUri);
        statement(statements, statementUri, uris.property(PropertyType.STATEMENT) + predicate, value);
        statement(statements, statementUri, Ontology.RANK, rank);
        return statements;
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
    @Test
    public void repeatedSiteLinksArentModified() throws QueryEvaluationException {
        /*
         * Note that this used to verify that noop updates for site links was
         * fast and now it just verifies that they don't cause any reported
         * modifications. Its not as strong an assertion but its less dependent
         * on external conditions.
         */
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String link = String.format(Locale.ROOT, "http://%s.example.com/wiki/tbl", i);
            statements.add(statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE));
            statements.add(statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(Integer.toString(i))));
            statements.add(statement(link, SchemaDotOrg.ABOUT, "Q80"));
        }
        rdfRepository.sync("Q80", statements);
        assertEquals(0, rdfRepository.sync("Q80", statements));
        TupleQueryResult r = rdfRepository.query("PREFIX wd: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?s) as ?sc) WHERE {?s ?p wd:Q80}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(10))));
        assertFalse(r.hasNext());
    }

    /**
     * Updating items with lots of statements shouldn't be painfully slow.
     */
    @Test
    public void repeatedStatementsArentModified() throws QueryEvaluationException {
        /*
         * Note that this used to verify that noop updates for statements was
         * fast and now it just verifies that they don't cause any reported
         * modifications. Its not as strong an assertion but its less dependent
         * on external conditions.
         */
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            statements.add(statement("Q80", "P" + i, new IntegerLiteralImpl(BigInteger.valueOf(i))));
        }
        rdfRepository.sync("Q80", statements);
        assertEquals(0, rdfRepository.sync("Q80", statements));
        TupleQueryResult r = rdfRepository.query("PREFIX wd: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?p) as ?sc) WHERE {wd:Q80 ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(10))));
        assertFalse(r.hasNext());
    }

    @Test
    public void delete() throws QueryEvaluationException {
        newSiteLink();
        rdfRepository.sync("Q23", Collections.<Statement> emptyList());
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertFalse(r.hasNext());
    }

    @Test
    public void fetchLeftOffTimeEmpty() {
        assertNull(rdfRepository.fetchLeftOffTime());
    }

    @Test
    public void updateLeftOffTimeFetch() {
        Instant now = Instant.now();
        rdfRepository.updateLeftOffTime(now);
        assertEquals(now, rdfRepository.fetchLeftOffTime());
    }

    private void syncJustVersion(String entityId, int version) {
        Statement statement = statement(entityId, SchemaDotOrg.VERSION,
                new IntegerLiteralImpl(new BigInteger(Integer.toString(version))));
        rdfRepository.sync(entityId, ImmutableList.of(statement));
    }

    @Test
    public void statementWithBnode() throws QueryEvaluationException {
        rdfRepository.sync("Q42", ImmutableList.of(//
                statement("Q42", "P396", new BNodeImpl("testBnode"))));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "Q42"), //
                binds("p", "P396"), //
                binds("o", BNode.class)));
        assertFalse(r.hasNext());
    }

    /**
     * Use Munger to create proper cleanup list.
     */
    private List<String> makeCleanupList(String entityId, List<Statement> data) {
        Set<String> values = new HashSet<>();
        Set<String> refs = new HashSet<>();
        munger.mungeWithValues(entityId, data,
                rdfRepository.getValues(Arrays.asList(uris.entity() + entityId)),
                rdfRepository.getRefs(Arrays.asList(uris.entity() + entityId)),
                values, refs, null);
        List<String> changeList = new ArrayList<>();
        changeList.addAll(values);
        changeList.addAll(refs);
        return changeList;
    }

    /**
     * Test cleaning up value node that is no more used.
     * @throws QueryEvaluationException
     */
    @Test
    public void cleanupUnusedValue() throws QueryEvaluationException {
        String statementUri = uris.statement() + randomAsciiOfLength(10);
        String valueUri = uris.value() + "changeduuid";
        expandedStatementWithExpandedValue();

        List<Statement> newdata = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeValue(valueUri, "dog")
                .withEntityData("22", "today")
                .build();

        rdfRepository.sync("Q23", newdata, makeCleanupList("Q23", newdata));

        assertTrue(rdfRepository.ask("ASK { wd:Q23 p:P509 [ psv:P509 wdv:changeduuid ] }"));
        assertTrue(rdfRepository.ask(
                "ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"dog\" ] ] }"));
        // Old one must be deleted
        assertFalse(rdfRepository.ask("ASK { wdv:someuuid ?x ?y }"));
    }

    /**
     * Test cleanup when one value is still used by other data.
     * @throws QueryEvaluationException
     */
    public void cleanupValueWithUsedByOld() throws QueryEvaluationException {
        String statementUri = uris.statement() + randomAsciiOfLength(10);
        String oldValueUri = uris.value() + "someuuid";
        String valueUri = uris.value() + "changeduuid";
        cleanupList.add(valueUri);
        List<Statement> olddata = new ArrayList<>();
        statement(olddata, statementUri,
                uris.property(PropertyType.STATEMENT_VALUE) + "P222",
                oldValueUri);
        rdfRepository.sync("Q2", olddata);
        expandedStatementWithExpandedValue();

        List<Statement> newdata = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeValue(valueUri, "duck")
                .withEntityData("22", "today")
                .build();

        rdfRepository.sync("Q23", newdata, makeCleanupList("Q23", newdata));

        // New values are present
        assertTrue(rdfRepository.ask("ASK { wd:Q23 p:P509 [ psv:P509 wdv:changeduuid ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"duck\" ] ] }"));
        // Old value still used so keep it
        assertTrue(rdfRepository.ask("ASK { wdv:someuuid ?x ?y }"));
    }

    /**
     * Test cleanup value which includes normalized component.
     * @throws QueryEvaluationException
     */
    public void cleanupNormalizedValue() throws QueryEvaluationException {
        String statementUri = uris.statement() + randomAsciiOfLength(10);
        String valueUri = uris.value() + "someuuid";
        String normValueUri = uris.value() + "normuuid";

        List<Statement> olddata = new StatementBuilder("Q24")
                .withStatement("P5", statementUri)
                .withStatementValueNormalized(valueUri)
                .withStatementValueNormalized(normValueUri)
                .withQuanitityValueNormalized(valueUri, "42", normValueUri, "128")
                .withEntityData("22", "today")
                .build();
        rdfRepository.sync("Q24", olddata);

        valueUri = valueUri + "NEW";
        normValueUri = normValueUri + "NEW";

        List<Statement> newdata = new StatementBuilder("Q24")
                .withStatement("P7", statementUri)
                .withStatementValueNormalized(valueUri)
                .withStatementValueNormalized(normValueUri)
                .withQuanitityValueNormalized(valueUri, "24", normValueUri, "-5")
                .withEntityData("23", "tomorrow")
                .build();
        rdfRepository.sync("Q24", olddata, makeCleanupList("Q24", newdata));

        // New values are present
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psv:P7 someuuidNEW ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psn:P7 normuuidNEW ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psv:P7 [ wikibase:quantityAmount \"24\" ] ] }"));
        // Old value is deleted together with normalized part
        assertFalse(rdfRepository.ask("ASK { wdv:someuuid ?x ?y }"));
        assertFalse(rdfRepository.ask("ASK { wdv:normuuid ?x ?y }"));
    }
}
