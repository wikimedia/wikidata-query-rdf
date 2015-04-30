package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.hasItems;
import static org.wikidata.query.rdf.tool.Matchers.subjectPredicateObjectMatchers;
import static org.wikidata.query.rdf.tool.StatementHelper.randomStatementsAbout;
import static org.wikidata.query.rdf.tool.TupleQueryResultHelper.toIterable;

import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

/**
 * Validates that many triples can be inserted, then retrieved in a single
 * query.
 */
public class MultipleResultsQueryIntegrationTest extends AbstractUpdateIntegrationTestBase {

    @Test
    public void test() throws QueryEvaluationException {
        String s = "Q" + randomInt();
        List<Statement> statements = randomStatementsAbout(s);

        rdfRepository().sync(s, statements);
        TupleQueryResult results = rdfRepository().query("SELECT * WHERE {?s ?p ?o}");

        assertThat(toIterable(results), hasItems(subjectPredicateObjectMatchers(statements)));
    }

}
