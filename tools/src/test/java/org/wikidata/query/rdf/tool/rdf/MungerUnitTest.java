package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.wikidata.query.rdf.tool.StatementHelper.siteLink;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
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
    private final Munger munger = new Munger(EntityData.WIKIDATA, Entity.WIKIDATA);

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
        statements.add(statement("Q23", "P509", "Q6"));
        munger.munge("Q23", statements);
        assertThat(statements, hasItem(statement("Q23", "P509", "Q6")));
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
        Statement rdfsDecl = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "en"));
        Statement skosDecl = statement("Q23", SKOS.PREF_LABEL, new LiteralImpl("foo", "en"));
        Statement schemaDecl = statement("Q23", SchemaDotOrg.NAME, new LiteralImpl("foo", "en"));

        List<Statement> statements = basicEntity("Q23");
        statements.addAll(ImmutableList.of(rdfsDecl, skosDecl, schemaDecl));
        munger.munge("Q23", statements);
        assertThat(statements, hasItem(rdfsDecl));
        assertThat(statements, not(hasItem(skosDecl)));
        assertThat(statements, not(hasItem(schemaDecl)));
    }

    @Test
    public void labelsOnOthersRemoved() throws ContainedException {
        Statement georgeDecl = statement("Q23", RDFS.LABEL, new LiteralImpl("george", "en"));
        Statement marthaDecl = statement("Q191789", RDFS.LABEL, new LiteralImpl("martha", "en"));

        List<Statement> statements = basicEntity("Q23");
        statements.add(georgeDecl);
        statements.add(marthaDecl);
        munger.munge("Q23", statements);
        assertThat(statements, hasItem(georgeDecl));
        assertThat(statements, not(hasItem(marthaDecl)));
    }

    // TODO statement and value subjects
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
        Statement enLabel = statement("Q23", predicate, new LiteralImpl("foo", "en"));
        Statement deLabel = statement("Q23", predicate, new LiteralImpl("foo", "de"));
        Statement itLabel = statement("Q23", predicate, new LiteralImpl("foo", "it"));
        Statement frLabel = statement("Q23", predicate, new LiteralImpl("foo", "fr"));
        george.add(enLabel);
        george.add(deLabel);
        george.add(itLabel);
        george.add(frLabel);
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
        Statement enLabel = statement("Q23", predicate, new LiteralImpl("foo", "en"));
        Statement deLabel = statement("Q23", predicate, new LiteralImpl("foo", "de"));
        Statement itLabel = statement("Q23", predicate, new LiteralImpl("foo", "it"));
        Statement frLabel = statement("Q23", predicate, new LiteralImpl("foo", "fr"));
        if (randomBoolean()) {
            Statement sneaky = statement("Q2344", predicate, new LiteralImpl("sneaky", "en"));
            george.add(sneaky);
        }
        george.add(enLabel);
        george.add(deLabel);
        george.add(itLabel);
        george.add(frLabel);
        munger.singleLabelMode("en", "de").munge("Q23", george);
        assertThat(george, hasItem(enLabel));
        assertThat(george, not(hasItem(itLabel)));
        assertThat(george, not(hasItem(frLabel)));
        assertThat(george, not(hasItem(deLabel)));
        george = basicEntity("Q23");
        george.add(enLabel);
        george.add(deLabel);
        george.add(itLabel);
        george.add(frLabel);
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
        Statement enLabel = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "en"));
        Statement deLabel = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "de"));
        Statement itLabel = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "it"));
        Statement frLabel = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "fr"));
        george.add(enLabel);
        george.add(deLabel);
        george.add(itLabel);
        george.add(frLabel);
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
        List<Statement> statements = new ArrayList<>();
        String entityData = EntityData.WIKIDATA.namespace() + entityId;
        // EntityData is all munged onto Entity
        statements.add(statement(entityData, SchemaDotOrg.ABOUT, entityId));
        statements.add(statement(entityData, SchemaDotOrg.VERSION, new LiteralImpl("a revision number I promise")));
        statements.add(statement(entityData, SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise")));
        return statements;
    }
}
