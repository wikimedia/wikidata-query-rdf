package org.wikidata.query.rdf.tool.rdf;

import static org.hamcrest.Matchers.equalTo;
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
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.ImmutableList;

@RunWith(RandomizedRunner.class)
public class MungerUnitTest extends RandomizedTest {
    private final Munger munger = new Munger(EntityData.WIKIDATA, Entity.WIKIDATA);

    @Test
    public void mungesEntityData() {
        List<Statement> statements = new ArrayList<>();
        String entityData = EntityData.WIKIDATA.namespace() + "Q23";
        // EntityData is all munged onto Entity
        statements.add(statement(entityData, SchemaDotOrg.ABOUT, "Q23"));
        statements.add(statement(entityData, SchemaDotOrg.VERSION, new LiteralImpl("a revision number I promise")));
        statements.add(statement(entityData, SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise")));

        // Stuff from entity isn't messed with
        boolean hasExtra = randomBoolean();
        if (hasExtra) {
            statements.add(statement("Q23", "P509", "Q6"));
        }

        munger.munge(statements);
        // This Matcher is so hard to build......
        ImmutableList.Builder<Matcher<? super Statement>> matchers = ImmutableList.builder();
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.VERSION, new LiteralImpl("a revision number I promise"))));
        matchers.add(equalTo(statement("Q23", SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise"))));
        if (hasExtra) {
            matchers.add(equalTo(statement("Q23", "P509", "Q6")));
        }
        assertThat(statements, Matchers.<Statement> containsInAnyOrder(matchers.build()));
    }
}
