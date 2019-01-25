package org.wikidata.query.rdf.tool.rdf;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

public class ClassifiedStatementsUnitTest {

    private final WikibaseUris uris = WikibaseUris.getURISystem();

    @Test
    public void statementsAreClassified() {
        ClassifiedStatements statements = new ClassifiedStatements(uris);
        String entityId = "Q1";
        statements.classify(singleton(entityStatement(entityId)), entityId);

        assertThat(statements.entityStatements).hasSize(1);
        assertThat(statements.statementStatements).hasSize(0);
        assertThat(statements.aboutStatements).hasSize(0);
        assertThat(statements.getDataSize()).isEqualTo(65L);

        statements.classify(singleton(statementStatement()), entityId);

        assertThat(statements.entityStatements).hasSize(1);
        assertThat(statements.statementStatements).hasSize(1);
        assertThat(statements.aboutStatements).hasSize(0);
        assertThat(statements.getDataSize()).isEqualTo(141L);

        statements.classify(singleton(aboutStatement()), entityId);

        assertThat(statements.entityStatements).hasSize(1);
        assertThat(statements.statementStatements).hasSize(1);
        assertThat(statements.aboutStatements).hasSize(1);
        assertThat(statements.getDataSize()).isEqualTo(199L);
    }

    @Test
    public void clearEmptiesEverything() {
        ClassifiedStatements statements = new ClassifiedStatements(uris);
        String entityId = "Q1";
        statements.classify(singleton(entityStatement(entityId)), entityId);
        statements.classify(singleton(statementStatement()), entityId);
        statements.classify(singleton(aboutStatement()), entityId);

        assertThat(statements.entityStatements).hasSize(1);
        assertThat(statements.statementStatements).hasSize(1);
        assertThat(statements.aboutStatements).hasSize(1);
        assertThat(statements.getDataSize()).isEqualTo(199L);

        statements.clear();

        assertThat(statements.entityStatements).hasSize(0);
        assertThat(statements.statementStatements).hasSize(0);
        assertThat(statements.aboutStatements).hasSize(0);
        assertThat(statements.getDataSize()).isEqualTo(0L);
    }

    private Statement entityStatement(String entityId) {
        return statement(uris.entity() + entityId, "P1", 0);
    }

    private Statement statementStatement() {
        return statement(uris.statement() + "Foo", "P1", 0);
    }

    private Statement aboutStatement() {
        return statement(uris.root() + "Bar", "P1", 0);
    }
}
