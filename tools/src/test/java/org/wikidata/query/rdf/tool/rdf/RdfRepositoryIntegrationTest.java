package org.wikidata.query.rdf.tool.rdf;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.test.StatementHelper.siteLink;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UriSchemeFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;
import org.wikidata.query.rdf.test.Randomizer;
import org.wikidata.query.rdf.test.StatementHelper.StatementBuilder;
import org.wikidata.query.rdf.tool.RdfRepositoryForTesting;

import com.google.common.collect.ImmutableList;

/**
 * Tests RdfRepository against a live RDF repository.
 */
@RunWith(Parameterized.class)
public class RdfRepositoryIntegrationTest {
    private Set<String> cleanupList = new HashSet<>();

    /**
     * Wikibase uris to test with.
     */
    private final WikibaseUris uris = UriSchemeFactory.getURISystem();
    private final Munger munger = Munger.builder(uris).build();

    @Rule
    public final Randomizer randomizer = new Randomizer();

    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq");

    @Before
    public void cleanList() {
        cleanupList = new HashSet<>();
    }

    public RdfRepositoryIntegrationTest(boolean syncMode) {
        rdfRepository.setSyncMode(syncMode);
    }

    @Parameters
    public static Collection<Object[]> syncModes() {
        return Arrays.asList(new Object[][] {
            {false}, {true}
        });
    }

    @Test
    public void newSiteLink() throws QueryEvaluationException {
        rdfRepository.syncWithMode("Q23", siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washington", "en"));
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
        rdfRepository.syncWithMode("Q23", siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washingmoved", "en"));
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s <http://schema.org/about> ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), allOf(//
                binds("s", "http://en.wikipedia.org/wiki/George_Washingmoved"), //
                binds("o", "Q23")));
        assertFalse(r.hasNext());
    }

    @Test
    public void newLabel() throws QueryEvaluationException {
        rdfRepository.syncWithMode("Q23", ImmutableList.of(//
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
        rdfRepository.syncWithMode("Q23", ImmutableList.of(//
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
        rdfRepository.syncWithMode("Q23", ImmutableList.of(//
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
        rdfRepository.syncWithMode("Q42", ImmutableList.of(//
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
        rdfRepository.syncWithMode("Q23", ImmutableList.of(link, onGeorge));
        rdfRepository.syncWithMode("Q191789", ImmutableList.of(onMartha));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P20 wd:Q494413 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P26 wd:Q191789 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q191789 p:P20 wd:Q731635 }"));

        rdfRepository.syncWithMode("Q23", ImmutableList.of(onGeorge));
        assertTrue(rdfRepository.ask("ASK {wd:Q23 p:P20 wd:Q494413 }"));
        assertFalse(rdfRepository.ask("ASK {wd:Q23 p:P26 wd:Q191789 }"));
        assertTrue(rdfRepository.ask("ASK {wd:Q191789 p:P20 wd:Q731635 }"));
    }

    @Test
    public void newLabelLanguage() throws QueryEvaluationException {
        newLabel();
        rdfRepository.syncWithMode("Q23", ImmutableList.of(//
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
        rdfRepository.syncWithMode("Q23", george);
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
        rdfRepository.syncWithMode("Q23", george);
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
        rdfRepository.syncWithMode("Q23", george);
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

        rdfRepository.syncWithMode("Q23", george);
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

        rdfRepository.syncWithMode("Q23", george);
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
        String referenceUri = uris.reference() + "8e0adc764d952b70662076cd296bd391e6e65a67";
        String valueUri = uris.value() + "someuuid";
        cleanupList.add(valueUri);
        cleanupList.add(referenceUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withReferenceValue(referenceUri, "P509", valueUri)
                .withReference(referenceUri, "P143", "Q328")
                .withTimeCalendarValue(valueUri, "cat", "animals")
                .build();

        rdfRepository.syncWithMode("Q23", george);
        List<Statement> enwiki = new ArrayList<>();
        statement(enwiki, "Q328", "P509", "Q328");
        rdfRepository.syncWithMode("Q328", enwiki);
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
        String referenceUri = uris.reference() + "559909327f1a31f4657f2b6bea7e890cafb5ff93";
        String valueUri = uris.value() + "someuuid2";
        cleanupList.add(valueUri);
        cleanupList.add(referenceUri);
        List<Statement> george = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withReferenceValue(referenceUri, "P509", valueUri)
                .withTimeCalendarValue(valueUri, "dog", "animals")
                .build();

        rdfRepository.syncWithMode("Q23", george, cleanupList);
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
        rdfRepository.syncWithMode("Q23", george, cleanupList);
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
        String referenceUri = uris.reference() + "e00b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P854", "http://example.com");
        rdfRepository.syncWithMode("Q23", george, cleanupList);
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
        String referenceUri = uris.reference() + "e00b7373814a0b74caa84a5fc2b1e3297060ab0f";
        List<Statement> george = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q23", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        cleanupList.add(referenceUri);
        statement(george, referenceUri, uris.property(PropertyType.REFERENCE) + "P143", "http://www.anb.org/articles/02/02-00332.html");
        rdfRepository.syncWithMode("Q23", george, cleanupList);
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
        rdfRepository.syncWithMode("Q23", george);
        List<Statement> dummy = expandedStatement("9D3713FF-7BCC-489F-9386-C7322C0AC284", "Q1234134", "P19", "Q494413",
                Ontology.NORMAL_RANK, referenceUri);
        dummy.add(refDecl);
        rdfRepository.syncWithMode("Q1234134", dummy, cleanupList);

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
        rdfRepository.syncWithMode("Q1234134", dummy, cleanupList);

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
        rdfRepository.syncWithMode("Q23", george, cleanupList);

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
        rdfRepository.syncWithMode("Q23", george, cleanupList);
        assertTrue(rdfRepository.ask("ASK {wdref:e36b7373814a0b74caa84a5fc2b1e3297060ab0f pr:P854 ?o }"));

        rdfRepository.syncWithMode("Q23", georgeWithoutSecondReference, cleanupList);
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

    private List<Statement> expandedStatement(String statementId, String subject, String predicate, Object value,
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
        rdfRepository.syncWithMode("Q80", statements);
        assertEquals(0, rdfRepository.syncWithMode("Q80", statements));
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
        rdfRepository.syncWithMode("Q80", statements);
        assertEquals(0, rdfRepository.syncWithMode("Q80", statements));
        TupleQueryResult r = rdfRepository.query("PREFIX wd: <http://www.wikidata.org/entity/>\nSELECT (COUNT(?p) as ?sc) WHERE {wd:Q80 ?p ?o}");
        assertTrue(r.hasNext());
        assertThat(r.next(), binds("sc", new IntegerLiteralImpl(BigInteger.valueOf(10))));
        assertFalse(r.hasNext());
    }

    @Test
    public void delete() throws QueryEvaluationException {
        newSiteLink();
        rdfRepository.syncWithMode("Q23", Collections.<Statement> emptyList());
        TupleQueryResult r = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        assertFalse(r.hasNext());
    }

    @Test
    public void fetchLeftOffTimeEmpty() {
        assertNull(rdfRepository.fetchLeftOffTime());
    }

    @Test
    public void updateLeftOffTimeFetch() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        rdfRepository.updateLeftOffTime(now);
        assertEquals(now, rdfRepository.fetchLeftOffTime());
    }

    @Test
    public void fetchLeftOffMultipleTimesDump() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant earlier = now.minusSeconds(30);
        Instant later = now.plusSeconds(30);
        UpdateBuilder b = new UpdateBuilder("INSERT DATA {" +
           "%dump% %dateModified% %ts1% . " +
           "%dump% %dateModified% %ts2% . " +
           "%dump% %dateModified% %ts3% . " +
           "}");
        b.bindValue("ts1", now);
        b.bindValue("ts2", earlier);
        b.bindValue("ts3", later);
        b.bindUri("dump", Ontology.DUMP);
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        rdfRepository.update(b.toString());
        assertEquals(earlier, rdfRepository.fetchLeftOffTime());
    }

    private void syncJustVersion(String entityId, int version) {
        Statement statement = statement(entityId, SchemaDotOrg.VERSION,
                new IntegerLiteralImpl(new BigInteger(Integer.toString(version))));
        rdfRepository.syncWithMode(entityId, ImmutableList.of(statement));
    }

    @Test
    public void statementWithBnode() throws QueryEvaluationException {
        rdfRepository.syncWithMode("Q42", ImmutableList.of(//
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
        String entityURI = uris.entityIdToURI(entityId);
        munger.mungeWithValues(entityId, data,
                rdfRepository.getValues(singletonList(entityURI)),
                rdfRepository.getRefs(singletonList(entityURI)),
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
        String statementUri = uris.statement() + randomizer.randomAsciiOfLength(10);
        String valueUri = uris.value() + "changeduuid";
        expandedStatementWithExpandedValue();

        List<Statement> newdata = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeValue(valueUri, "dog")
                .withEntityData("22", "2016-01-08T00:00:00Z")
                .build();

        rdfRepository.syncWithMode("Q23", newdata, makeCleanupList("Q23", newdata));

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
    @Test
    public void cleanupValueWithUsedByOld() throws QueryEvaluationException {
        String statementUri = uris.statement() + randomizer.randomAsciiOfLength(10);
        String oldValueUri = uris.value() + "someuuid";
        String valueUri = uris.value() + "changeduuid";
        cleanupList.add(valueUri);
        List<Statement> olddata = new ArrayList<>();
        statement(olddata, statementUri,
                uris.property(PropertyType.STATEMENT_VALUE) + "P222",
                oldValueUri);
        rdfRepository.syncWithMode("Q2", olddata);
        expandedStatementWithExpandedValue();

        List<Statement> newdata = new StatementBuilder("Q23")
                .withStatement("P509", statementUri)
                .withStatementValue(valueUri)
                .withTimeValue(valueUri, "duck")
                .withEntityData("22", "2016-01-08T00:00:00Z")
                .build();

        rdfRepository.syncWithMode("Q23", newdata, makeCleanupList("Q23", newdata));

        // New values are present
        assertTrue(rdfRepository.ask("ASK { wd:Q23 p:P509 [ psv:P509 wdv:changeduuid ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q23 p:P509 [ psv:P509 [ wikibase:timeValue \"duck\" ] ] }"));
        // Old value still used so keep it
        assertTrue(rdfRepository.ask("ASK { wdv:someuuid ?x ?y }"));
    }

    /**
     * Test cleanup value which includes normalized component.
     */
    @Test
    public void cleanupNormalizedValue() {
        String statementUri = uris.statement() + randomizer.randomAsciiOfLength(10);
        String valueUri = uris.value() + "someuuid";
        String normValueUri = uris.value() + "normuuid";

        List<Statement> olddata = new StatementBuilder("Q24")
                .withStatement("P5", statementUri)
                .withStatementValue(valueUri)
                .withStatementValueNormalized(normValueUri)
                .withQuanitityValueNormalized(valueUri, "42", normValueUri, "128")
                .withEntityData("22", "2016-01-08T00:00:00Z")
                .build();
        rdfRepository.syncWithMode("Q24", olddata);

        valueUri = valueUri + "NEW";
        normValueUri = normValueUri + "NEW";

        List<Statement> newdata = new StatementBuilder("Q24")
                .withStatement("P7", statementUri)
                .withStatementValue(valueUri)
                .withStatementValueNormalized(normValueUri)
                .withQuanitityValueNormalized(valueUri, "24", normValueUri, "-5")
                .withEntityData("23", "2018-11-08T23:29:06Z")
                .build();
        rdfRepository.syncWithMode("Q24", newdata, makeCleanupList("Q24", newdata));

        // New values are present
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psv:P7 wdv:someuuidNEW ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psn:P7 wdv:normuuidNEW ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:Q24 p:P7 [ psv:P7 [ wikibase:quantityAmount \"24\" ] ] }"));
        // Old value is deleted together with normalized part
        assertFalse(rdfRepository.ask("ASK { wdv:someuuid ?x ?y }"));
        assertFalse(rdfRepository.ask("ASK { wdv:normuuid ?x ?y }"));
    }

    private void formWithStatement(List<Statement> statements, String formId, String repr,
            String statementUri, String propertyId, String value) {
        statement(statements, formId, Ontolex.REPRESENTATION, new LiteralImpl(repr));
        statements.addAll(expandedStatement(statementUri, formId, propertyId,
                new LiteralImpl(value), Ontology.NORMAL_RANK));
    }

    private void senseWithStatement(List<Statement> statements, String formId, String repr,
            String statementUri, String propertyId, String value) {
        statement(statements, formId, SKOS.DEFINITION, new LiteralImpl(repr));
        statements.addAll(expandedStatement(statementUri, formId, propertyId,
                new LiteralImpl(value), Ontology.NORMAL_RANK));
    }

    /**
     * Test lexemes updating & cleanup of old values.
     */
    @Test
    public void lexemeUpdate() throws QueryEvaluationException {
        String statementUri = uris.statement() + randomizer.randomAsciiOfLength(10);
        String statementUri2 = uris.statement() + randomizer.randomAsciiOfLength(10);
        String statementUri3 = uris.statement() + randomizer.randomAsciiOfLength(10);
        String statementUri4 = uris.statement() + randomizer.randomAsciiOfLength(10);
        String statementUri5 = uris.statement() + randomizer.randomAsciiOfLength(10);

        List<Statement> olddata = new StatementBuilder("L123")
                .withPredicateObject(Ontology.Lexeme.LEMMA, new LiteralImpl("testlemma"))
                .withPredicateObject(Ontolex.LEXICAL_FORM, "L123-F1")
                .withPredicateObject(Ontolex.LEXICAL_FORM, "L123-F2")
                .withPredicateObject(Ontolex.SENSE_PREDICATE, "L123-S1")
                .build();

        formWithStatement(olddata, "L123-F1", "form1test", statementUri, "P12", "formvalue");
        formWithStatement(olddata, "L123-F2", "form2test", statementUri2, "P23", "formvalue2");
        senseWithStatement(olddata, "L123-S1", "sensetest", statementUri3, "P12", "sensevalue");

        rdfRepository.syncWithMode("L123", olddata);

        List<Statement> newdata = new StatementBuilder("L123")
                .withPredicateObject(Ontology.Lexeme.LEMMA, new LiteralImpl("test2lemma"))
                .withPredicateObject(Ontolex.LEXICAL_FORM, "L123-F1")
                .withPredicateObject(Ontolex.SENSE_PREDICATE, "L123-S1")
                .withPredicateObject(Ontolex.SENSE_PREDICATE, "L123-S2")
                .build();

        formWithStatement(newdata, "L123-F1", "form1new", statementUri5, "P12", "formnewvalue");
        senseWithStatement(newdata, "L123-S1", "sense1new", statementUri3, "P34", "sensenewvalue");
        senseWithStatement(newdata, "L123-S2", "sense2new", statementUri4, "P123", "sensenew2value");

        rdfRepository.syncWithMode("L123", newdata);
        // New data is here
        assertTrue(rdfRepository.ask("ASK { wd:L123 wikibase:lemma \"test2lemma\" }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/ontolex:representation \"form1new\" }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/p:P12 [ ps:P12 \"formnewvalue\" ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:sense/skos:definition \"sense1new\" }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:sense/skos:definition \"sense2new\" }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:sense/p:P34 [ ps:P34 \"sensenewvalue\" ] }"));
        assertTrue(rdfRepository.ask("ASK { wd:L123 ontolex:sense/p:P123 [ ps:P123 \"sensenew2value\" ] }"));
        // Old data is not
        assertFalse(rdfRepository.ask("ASK { wd:L123 wikibase:lemma \"testlemma\" }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/ontolex:representation \"form1test\" }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/ontolex:representation \"form2test\" }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/p:P12 [ ps:P12 \"formvalue\" ] }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:lexicalForm/p:P23 [ ] }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:sense/skos:definition \"sensetest\" }"));
        assertFalse(rdfRepository.ask("ASK { wd:L123 ontolex:sense/p:P12 [ ] }"));
    }
}
