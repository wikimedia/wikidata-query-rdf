package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
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
    public void mungesEntityDataOntoEntity() {
        List<Statement> statements = basicEntity("Q23");

        munger.munge(statements);
        // This Matcher is so hard to build......
        ImmutableList.Builder<Matcher<? super Statement>> matchers = ImmutableList.builder();
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.VERSION, new LiteralImpl("a revision number I promise"))));
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise"))));
        assertThat(statements, Matchers.<Statement> containsInAnyOrder(matchers.build()));
    }

    @Test
    public void extraDataIsntModified() {
        List<Statement> statements = basicEntity("Q23");
        statements.add(statement("Q23", "P509", "Q6"));
        munger.munge(statements);
        assertThat(statements, hasItem(equalTo(statement("Q23", "P509", "Q6"))));
    }

    @Test(expected = BadSubjectException.class)
    public void complainsAboutExtraSubjects() {
        List<Statement> statements = basicEntity("Q23");
        statements.add(statement("http://example.com/bogus", "Q23", "Q23"));
        munger.munge(statements);
    }

    @Test
    public void siteLinksGoThrough() {
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
        munger.munge(statements);
        assertThat(statements, both(hasItem(equalTo(articleDecl))).and(hasItem(equalTo(metaDecl))));
    }

    @Test
    public void extraLabelsRemoved() {
        Statement rdfsDecl = statement("Q23", RDFS.LABEL, new LiteralImpl("foo", "en"));
        Statement skosDecl = statement("Q23", SKOS.PREF_LABEL, new LiteralImpl("foo", "en"));
        Statement schemaDecl = statement("Q23", SchemaDotOrg.NAME, new LiteralImpl("foo", "en"));

        List<Statement> statements = basicEntity("Q23");
        statements.addAll(ImmutableList.of(rdfsDecl, skosDecl, schemaDecl));
        munger.munge(statements);
        assertThat(statements, hasItem(equalTo(rdfsDecl)));
        assertThat(statements, not(hasItem(equalTo(skosDecl))));
        assertThat(statements, not(hasItem(equalTo(schemaDecl))));
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
