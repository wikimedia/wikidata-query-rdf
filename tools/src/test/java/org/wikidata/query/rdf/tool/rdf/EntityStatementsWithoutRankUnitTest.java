package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.tool.rdf.EntityStatementsWithoutRank.entityStatementsWithoutRank;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.test.StatementHelper;

public class EntityStatementsWithoutRankUnitTest {
    @Test
    public void test() {
        List<Statement> statements = new ArrayList<>();
        statements.add(StatementHelper.statement("foo:stmtId1", RDF.TYPE, Ontology.STATEMENT));
        assertThat(statements.stream().collect(entityStatementsWithoutRank())).containsExactly("foo:stmtId1");

        statements.add(StatementHelper.statement("foo:stmtId1", Ontology.RANK, Ontology.BEST_RANK));
        assertThat(statements.stream().collect(entityStatementsWithoutRank())).isEmpty();

        statements.add(StatementHelper.statement("foo:stmtId2", RDF.TYPE, Ontology.STATEMENT));
        statements.add(StatementHelper.statement("foo:stmtId3", RDF.TYPE, Ontology.STATEMENT));
        assertThat(statements.stream().collect(entityStatementsWithoutRank()))
                .containsExactlyInAnyOrder("foo:stmtId2", "foo:stmtId3");
    }
}
