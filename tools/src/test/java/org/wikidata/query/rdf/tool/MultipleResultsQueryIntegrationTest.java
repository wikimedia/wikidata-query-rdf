package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.test.Matchers.subjectPredicateObjectMatchers;
import static org.wikidata.query.rdf.test.StatementHelper.randomStatementsAbout;
import static org.wikidata.query.rdf.tool.TupleQueryResultHelper.toIterable;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.test.Randomizer;

import com.google.common.collect.Lists;

/**
 * Validates that many triples can be inserted, then retrieved in a single
 * query.
 */
public class MultipleResultsQueryIntegrationTest extends AbstractUpdaterIntegrationTestBase {

    @Rule
    public final Randomizer randomizer = new Randomizer();

    private static final int MAX_STATEMENT_COUNT = 100;

    private void expect(List<Statement> present, List<Statement> absent) throws QueryEvaluationException {
        TupleQueryResult tupleQueryResult = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        Iterable<BindingSet> results = toIterable(tupleQueryResult);
        assertThat(results, hasItems(subjectPredicateObjectMatchers(present)));
        for (Matcher<BindingSet> matcher : subjectPredicateObjectMatchers(absent)) {
            assertThat(results, not(hasItem(matcher)));
        }
    }

    @Test
    public void testInsertsAndDeletes() throws QueryEvaluationException {
        String s = "Q" + randomizer.randomInt();

        List<Statement> statements1 = randomStatementsAbout(randomizer, s, randomizer.randomIntBetween(1, MAX_STATEMENT_COUNT));
        List<Statement> statements2 = randomStatementsAbout(randomizer, s, randomizer.randomIntBetween(1, MAX_STATEMENT_COUNT));

        rdfRepository.sync(s, statements1);
        expect(statements1, statements2);

        List<Statement> all = Lists.newArrayList();
        all.addAll(statements1);
        all.addAll(statements2);
        rdfRepository.sync(s, all);
        expect(all, new ArrayList<Statement>());

        rdfRepository.sync(s, statements2);
        expect(statements2, statements1);
    }

    @Test
    public void testInserts() throws QueryEvaluationException {
        String s = "Q" + randomizer.randomInt();
        int statementCount = randomizer.randomIntBetween(1, MAX_STATEMENT_COUNT);
        List<Statement> statements = randomStatementsAbout(randomizer, s, statementCount);

        rdfRepository.sync(s, statements);
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");

        assertThat(toIterable(results), hasItems(subjectPredicateObjectMatchers(statements)));
    }

}
