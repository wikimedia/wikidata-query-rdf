package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.wikidata.query.rdf.tool.StatementHelper.siteLink;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.rdf.Munger.BadSubjectException;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.ImmutableList;

/**
 * Tests Munger.
 */
@RunWith(RandomizedRunner.class)
public class MungerUnitTest extends RandomizedTest {
    private final WikibaseUris uris = WikibaseUris.WIKIDATA;
    private final Munger munger = new Munger(uris);

    @Test
    public void mungesEntityDataOntoEntity() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        munger.munge("Q23", statements);
        // This Matcher is so hard to build......
        ImmutableList.Builder<Matcher<? super Statement>> matchers = ImmutableList.builder();
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.VERSION, new LiteralImpl("a revision number I promise"))));
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise"))));
        assertThat(statements, Matchers.<Statement> containsInAnyOrder(matchers.build()));
    }

    @Test
    public void extraDataIsntModified() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        Statement extra = statement(statements, "Q23", "P509", "Q6");
        munger.munge("Q23", statements);
        assertThat(statements, hasItem(extra));
    }

    @Test
    public void aItemIsRemoved() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        Statement aItemDecl = statement(statements, "Q23", RDF.TYPE, Ontology.ITEM);
        munger.munge("Q23", statements);
        assertThat(statements, not(hasItem(aItemDecl)));
    }

    @Test(expected = BadSubjectException.class)
    public void complainsAboutExtraSubjects() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        statements.add(statement("http://example.com/bogus", "Q23", "Q23"));
        munger.munge("Q23", statements);
    }

    @Test
    public void siteLinksGoThrough() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        String bogus = "http://example.com/bogus";
        Statement articleDecl = statement(bogus, RDF.TYPE, SchemaDotOrg.ARTICLE);
        Statement metaDecl = statement(bogus, "Q23", new LiteralImpl("Doesn't matter"));
        if (randomBoolean()) {
            statements.add(articleDecl);
            statements.add(metaDecl);
        } else {
            // Out of order should be ok too
            statements.add(metaDecl);
            statements.add(articleDecl);
        }
        munger.munge("Q23", statements);
        assertThat(statements, both(hasItem(articleDecl)).and(hasItem(metaDecl)));
    }

    @Test
    public void extraLabelsRemoved() throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        Statement rdfsDecl = statement(george, "Q23", RDFS.LABEL, new LiteralImpl("foo", "en"));
        Statement skosDecl = statement(george, "Q23", SKOS.PREF_LABEL, new LiteralImpl("foo", "en"));
        Statement schemaDecl = statement(george, "Q23", SchemaDotOrg.NAME, new LiteralImpl("foo", "en"));
        munger.munge("Q23", george);
        assertThat(george, hasItem(rdfsDecl));
        assertThat(george, not(hasItem(skosDecl)));
        assertThat(george, not(hasItem(schemaDecl)));
    }

    @Test
    public void labelsOnOthersRemoved() throws ContainedException {
        List<Statement> statements = basicEntity("Q23");
        Statement georgeDecl = statement(statements, "Q23", RDFS.LABEL, new LiteralImpl("george", "en"));
        Statement marthaDecl = statement(statements, "Q191789", RDFS.LABEL, new LiteralImpl("martha", "en"));
        munger.munge("Q23", statements);
        assertThat(statements, hasItem(georgeDecl));
        assertThat(statements, not(hasItem(marthaDecl)));
    }

    @Test
    public void basicExpandedStatement() throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        String statementUri = uris.statement() + "Q23-ce976010-412f-637b-c687-9fd2d52dc140";
        Statement statementTypeDecl = statement(george, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(george, statementUri, uris.value() + "P509", "Q356405");
        Statement rankDecl = statement(george, statementUri, Ontology.RANK, Ontology.NORMAL_RANK);
        Statement statementDecl = statement("Q23", "P509", statementUri);
        if (randomBoolean()) {
            george.add(0, statementDecl);
        } else {
            george.add(statementDecl);
        }
        munger.munge("Q23", george);
        assertThat(george, hasItem(statementDecl));
        assertThat(george, not(hasItem(statementTypeDecl)));
        assertThat(george, hasItem(valueDecl));
        assertThat(george, hasItem(rankDecl));
        // TODO can we rewrite the valueDecl into something without the repeated
        // property?
    }

    @Test
    public void expandedStatementWithReference() throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        String statementUri = uris.statement() + "Q23-9D3713FF-7BCC-489F-9386-C7322C0AC284";
        String referenceUri = uris.reference() + "e36b7373814a0b74caa84a5fc2b1e3297060ab0f";
        Statement statementDecl = statement(george, "Q23", "P19", statementUri);
        Statement statementTypeDecl = statement(george, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(george, statementUri, uris.value() + "P19", "Q494413");
        Statement rankDecl = statement(george, statementUri, Ontology.RANK, Ontology.NORMAL_RANK);
        Statement referenceTypeDecl = statement(george, referenceUri, RDF.TYPE, Ontology.REFERENCE);
        Statement referenceValueDecl = statement(george, referenceUri, uris.value() + "P854",
                "http://www.anb.org/articles/02/02-00332.html");
        Statement referenceDecl = statement(statementUri, Provenance.WAS_DERIVED_FROM, referenceUri);
        if (randomBoolean()) {
            george.add(0, referenceDecl);
        } else {
            george.add(referenceDecl);
        }
        munger.munge("Q23", george);
        assertThat(george, hasItem(statementDecl));
        assertThat(george, not(hasItem(statementTypeDecl)));
        assertThat(george, hasItem(valueDecl));
        assertThat(george, hasItem(rankDecl));
        assertThat(george, hasItem(referenceDecl));
        assertThat(george, not(hasItem(referenceTypeDecl)));
        assertThat(george, hasItem(referenceValueDecl));
    }

    @Test
    public void expandedStatementWithQualifier() throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        String statementUri = uris.statement() + "q23-8A2F4718-6159-4E58-A8F9-6F24F5EFEC42";
        Statement statementDecl = statement(george, "Q23", "P26", statementUri);
        Statement statementTypeDecl = statement(george, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(george, statementUri, uris.value() + "P26", "Q191789");
        Statement rankDecl = statement(george, statementUri, Ontology.RANK, Ontology.NORMAL_RANK);
        Statement qualifierDecl = statement(george, statementUri, uris.qualifier() + "P580",
                new LiteralImpl("1759-01-06T00:00:00Z", XMLSchema.DATETIME));
        munger.munge("Q23", george);
        assertThat(george, hasItem(statementDecl));
        assertThat(george, not(hasItem(statementTypeDecl)));
        assertThat(george, hasItem(valueDecl));
        assertThat(george, hasItem(rankDecl));
        assertThat(george, hasItem(qualifierDecl));
    }

    @Test
    public void basicExpandedValue() {
        List<Statement> universe = basicEntity("Q1");
        String statementUri = uris.statement() + "q1-someuuid";
        String valueUri = uris.value() + "someotheruuid";
        Statement statementDecl = statement(universe, "Q1", "P580", statementUri);
        Statement statementTypeDecl = statement(universe, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(universe, statementUri, uris.value() + "P580",
                new LiteralImpl("-13798000000-01-01T00:00:00Z", XMLSchema.DATETIME));
        Statement expandedValueDecl = statement(universe, statementUri, uris.value() + "P580"
                + "-value", valueUri);
        Statement expandedValueTypeDecl = statement(universe, valueUri, RDF.TYPE, Ontology.VALUE);
        /*
         * Currently wikibase exports the deep time values as strings, not
         * dateTime.
         */
        Statement expandedValueValueDecl = statement(universe, valueUri, Ontology.Time.VALUE,
                "-13798000000-01-01T00:00:00Z");
        Statement expandedValuePrecisionDecl = statement(universe, valueUri, Ontology.Time.PRECISION,
                new IntegerLiteralImpl(BigInteger.valueOf(3)));
        Statement expandedValueTimezoneDecl = statement(universe, valueUri, Ontology.Time.TIMEZONE,
                new IntegerLiteralImpl(BigInteger.valueOf(0)));
        Statement expandedValueCalendarModelDecl = statement(universe, valueUri, Ontology.Time.CALENDAR_MODEL,
                "Q1985727");
        munger.munge("Q1", universe);
        assertThat(universe, hasItem(statementDecl));
        assertThat(universe, not(hasItem(statementTypeDecl)));
        assertThat(universe, hasItem(valueDecl));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, not(hasItem(expandedValueTypeDecl)));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, hasItem(expandedValueValueDecl));
        assertThat(universe, hasItem(expandedValuePrecisionDecl));
        assertThat(universe, hasItem(expandedValueTimezoneDecl));
        assertThat(universe, hasItem(expandedValueCalendarModelDecl));
    }

    @Test
    public void expandedValueOnQualifier() {
        List<Statement> universe = basicEntity("Q1");
        String statementUri = uris.statement() + "q1-someuuid";
        String valueUri = uris.value() + "someotheruuid";
        Statement statementDecl = statement(universe, "Q1", "P580", statementUri);
        Statement statementTypeDecl = statement(universe, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(universe, statementUri, uris.qualifier() + "P580",
                new LiteralImpl("-13798000000-01-01T00:00:00Z", XMLSchema.DATETIME));
        Statement expandedValueDecl = statement(universe, statementUri, uris.value() + "P580"
                + "-value", valueUri);
        Statement expandedValueTypeDecl = statement(universe, valueUri, RDF.TYPE, Ontology.VALUE);
        /*
         * Currently wikibase exports the deep time values as strings, not
         * dateTime.
         */
        Statement expandedValueValueDecl = statement(universe, valueUri, Ontology.Time.VALUE,
                "-13798000000-01-01T00:00:00Z");
        Statement expandedValuePrecisionDecl = statement(universe, valueUri, Ontology.Time.PRECISION,
                new IntegerLiteralImpl(BigInteger.valueOf(3)));
        Statement expandedValueTimezoneDecl = statement(universe, valueUri, Ontology.Time.TIMEZONE,
                new IntegerLiteralImpl(BigInteger.valueOf(0)));
        Statement expandedValueCalendarModelDecl = statement(universe, valueUri, Ontology.Time.CALENDAR_MODEL,
                "Q1985727");
        munger.munge("Q1", universe);
        assertThat(universe, hasItem(statementDecl));
        assertThat(universe, not(hasItem(statementTypeDecl)));
        assertThat(universe, hasItem(valueDecl));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, not(hasItem(expandedValueTypeDecl)));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, hasItem(expandedValueValueDecl));
        assertThat(universe, hasItem(expandedValuePrecisionDecl));
        assertThat(universe, hasItem(expandedValueTimezoneDecl));
        assertThat(universe, hasItem(expandedValueCalendarModelDecl));
    }

    @Test
    public void basicExpandedValueOnReference() {
        List<Statement> universe = basicEntity("Q1");
        String statementUri = uris.statement() + "q1-someuuid";
        String valueUri = uris.value() + "someotheruuid";
        String referenceUri = uris.reference() + "yetanotheruuid";
        Statement statementDecl = statement(universe, "Q1", "P580", statementUri);
        Statement statementTypeDecl = statement(universe, statementUri, RDF.TYPE, Ontology.STATEMENT);
        Statement valueDecl = statement(universe, statementUri, uris.value() + "P580",
                new LiteralImpl("-13798000000-01-01T00:00:00Z", XMLSchema.DATETIME));
        Statement referenceDecl = statement(universe, statementUri, Provenance.WAS_DERIVED_FROM, referenceUri);
        Statement referenceTypeDecl = statement(universe, referenceUri, RDF.TYPE, Ontology.REFERENCE);
        Statement expandedValueDecl = statement(universe, referenceUri, uris.value() + "P580"
                + "-value", valueUri);
        Statement expandedValueTypeDecl = statement(universe, valueUri, RDF.TYPE, Ontology.VALUE);
        /*
         * Currently wikibase exports the deep time values as strings, not
         * dateTime.
         */
        Statement expandedValueValueDecl = statement(universe, valueUri, Ontology.Time.VALUE,
                "-13798000000-01-01T00:00:00Z");
        Statement expandedValuePrecisionDecl = statement(universe, valueUri, Ontology.Time.PRECISION,
                new IntegerLiteralImpl(BigInteger.valueOf(3)));
        Statement expandedValueTimezoneDecl = statement(universe, valueUri, Ontology.Time.TIMEZONE,
                new IntegerLiteralImpl(BigInteger.valueOf(0)));
        Statement expandedValueCalendarModelDecl = statement(universe, valueUri, Ontology.Time.CALENDAR_MODEL,
                "Q1985727");
        munger.munge("Q1", universe);
        assertThat(universe, hasItem(statementDecl));
        assertThat(universe, not(hasItem(statementTypeDecl)));
        assertThat(universe, hasItem(valueDecl));
        assertThat(universe, hasItem(referenceDecl));
        assertThat(universe, not(hasItem(referenceTypeDecl)));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, not(hasItem(expandedValueTypeDecl)));
        assertThat(universe, hasItem(expandedValueDecl));
        assertThat(universe, hasItem(expandedValueValueDecl));
        assertThat(universe, hasItem(expandedValuePrecisionDecl));
        assertThat(universe, hasItem(expandedValueTimezoneDecl));
        assertThat(universe, hasItem(expandedValueCalendarModelDecl));
    }
    // TODO somevalue and novalue

    // TODO badges
    @Test
    public void limitLanguagesLabel() throws ContainedException {
        limitLanguagesTestCase(RDFS.LABEL);
    }

    @Test
    public void limitLanguagesDescription() throws ContainedException {
        limitLanguagesTestCase(SchemaDotOrg.DESCRIPTION);
    }

    @Test
    public void limitLanguagesAlias() throws ContainedException {
        limitLanguagesTestCase(SKOS.ALT_LABEL);
    }

    private void limitLanguagesTestCase(String predicate) throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        Statement enLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "en"));
        Statement deLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "de"));
        Statement itLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "it"));
        Statement frLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "fr"));
        munger.limitLabelLanguages("en", "de").munge("Q23", george);
        assertThat(george, not(hasItem(itLabel)));
        assertThat(george, not(hasItem(frLabel)));
        assertThat(george, hasItem(enLabel));
        assertThat(george, hasItem(deLabel));
    }

    @Test
    public void singleLabelModeLabel() throws ContainedException {
        singleLabelModeTestCase(RDFS.LABEL);
    }

    @Test
    public void singleLabelModeDescription() throws ContainedException {
        singleLabelModeTestCase(SchemaDotOrg.DESCRIPTION);
    }

    private void singleLabelModeTestCase(String predicate) throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        Statement enLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "en"));
        Statement deLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "de"));
        Statement itLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "it"));
        Statement frLabel = statement(george, "Q23", predicate, new LiteralImpl("foo", "fr"));
        /*
         * Extra garbage entityData information shouldn't break the single label
         * mode.
         */
        if (randomBoolean()) {
            george.addAll(basicEntity("Q44"));
        }
        if (randomBoolean()) {
            george.addAll(0, basicEntity("Q78"));
        }
        /*
         * Neither should labels for other entities.
         */
        if (randomBoolean()) {
            Statement sneaky = statement("Q2344", predicate, new LiteralImpl("sneaky", "en"));
            george.add(sneaky);
        }
        List<Statement> otherGeorge = new ArrayList<>(george);
        munger.singleLabelMode("en", "de").munge("Q23", george);
        assertThat(george, hasItem(enLabel));
        assertThat(george, not(hasItem(itLabel)));
        assertThat(george, not(hasItem(frLabel)));
        assertThat(george, not(hasItem(deLabel)));
        george = otherGeorge;
        munger.singleLabelMode("ja").munge("Q23", george);
        assertThat(george, hasItem(statement("Q23", RDFS.LABEL, "Q23")));
        assertThat(george, not(hasItem(enLabel)));
        assertThat(george, not(hasItem(itLabel)));
        assertThat(george, not(hasItem(frLabel)));
        assertThat(george, not(hasItem(deLabel)));
    }

    /**
     * Combined single label mode with limit label languages. The trouble with
     * doing both is that sometimes statements are removed twice.
     */
    @Test
    public void singleLabelAndLimitLanguage() throws ContainedException {
        List<Statement> george = basicEntity("Q23");
        Statement enLabel = statement(george, "Q23", RDFS.LABEL, new LiteralImpl("foo", "en"));
        Statement deLabel = statement(george, "Q23", RDFS.LABEL, new LiteralImpl("foo", "de"));
        Statement itLabel = statement(george, "Q23", RDFS.LABEL, new LiteralImpl("foo", "it"));
        Statement frLabel = statement(george, "Q23", RDFS.LABEL, new LiteralImpl("foo", "fr"));
        munger.singleLabelMode("en", "de").limitLabelLanguages("en").munge("Q23", george);
        assertThat(george, hasItem(enLabel));
        assertThat(george, not(hasItem(itLabel)));
        assertThat(george, not(hasItem(frLabel)));
        assertThat(george, not(hasItem(deLabel)));
    }

    @Test
    public void skipSiteLinks() throws ContainedException {
        List<Statement> siteLink = siteLink("Q23", "http://en.wikipedia.org/wiki/George_Washington", "en",
                randomBoolean());
        List<Statement> george = basicEntity("Q23");
        george.addAll(siteLink);
        munger.removeSiteLinks().munge("Q23", george);
        for (Statement siteLinkPart : siteLink) {
            assertThat(george, not(hasItem(siteLinkPart)));
        }
    }

    private List<Statement> basicEntity(String entityId) {
        return basicEntity(entityId, new LiteralImpl("a revision number I promise"));
    }

    private List<Statement> basicEntity(String entityId, Literal version) {
        List<Statement> statements = new ArrayList<>();
        String entityDataUri = uris.entityData() + entityId;
        // EntityData is all munged onto Entity
        statement(statements, entityDataUri, SchemaDotOrg.ABOUT, entityId);
        statement(statements, entityDataUri, SchemaDotOrg.VERSION, version);
        statement(statements, entityDataUri, SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise"));
        return statements;
    }
}
