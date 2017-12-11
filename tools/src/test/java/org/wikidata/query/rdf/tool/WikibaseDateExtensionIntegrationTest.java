package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

/**
 * Validates the WikibaseDateExtension over the Blazegraph API.
 */
public class WikibaseDateExtensionIntegrationTest extends AbstractUpdaterIntegrationTestBase {
    /**
     * Loads Q1 (universe) and validates that it can find it by searching for
     * things before some date very far in the past. Without our date extension
     * the start time doesn't properly parse in Blazegraph and doesn't allow
     * less than operations.
     */
    @Test
    public void bigBang() throws QueryEvaluationException {
        update(1, 1);
        StringBuilder query = new StringBuilder();
        query.append("PREFIX assert: <http://www.wikidata.org/prop/direct/>\n");
        query.append("SELECT * WHERE {\n");
        query.append("?s assert:P580 ?startTime .\n");
        query.append("FILTER (?startTime < \"-04540000000-01-01");
        if (randomBoolean()) {
            query.append("T00:00:00Z");
        }
        query.append("\"^^xsd:dateTime)\n");
        query.append("}");
        TupleQueryResult results = rdfRepository.query(query.toString());
        assertTrue(results.hasNext());
        BindingSet result = results.next();
        assertThat(result, binds("s", "Q1"));
        assertThat(result, binds("startTime", new LiteralImpl("-13798000000-01-01T00:00:00Z", XMLSchema.DATETIME)));
    }

    @Test
    public void date() throws QueryEvaluationException {
        List<Statement> statements = new ArrayList<>();
        statements.add(statement("Q23", "P569", new LiteralImpl("1732-02-22", XMLSchema.DATE)));
        rdfRepository.sync("Q23", statements);
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {?s ?p ?o}");
        BindingSet result = results.next();
        assertThat(result, binds("s", "Q23"));
        assertThat(result, binds("p", "P569"));
        assertThat(result, binds("o", new LiteralImpl("1732-02-22", XMLSchema.DATE)));
    }

    /**
     * This checks that date math works according to XML 1.1 standard.
     * Year 0 exists and is 1BCE.
     * @throws QueryEvaluationException
     */
    @Test
    public void dateMath() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query(
            "SELECT * WHERE {BIND ( \"0001-01-01T00:00:00\"^^xsd:dateTime - \"-0001-01-01T00:00:00\"^^xsd:dateTime AS ?date)}");
        BindingSet result = results.next();
        // 731 days or 2 years since XML 1.1 has year 0 (which is 1BCE)
        assertThat(result, binds("date", new LiteralImpl("731.0", XMLSchema.DOUBLE)));
    }

    @Test
    public void dateMathMore() throws QueryEvaluationException {
        String query = "prefix schema: <http://schema.org/>\n" +
            "INSERT {\n" +
            "<http://test.com/a> schema:lastModified \"0001-01-01T00:00:00\"^^xsd:dateTime .\n" +
            "<http://test.com/a> schema:lastModified \"-0001-01-01\"^^xsd:date .\n" +
            "<http://test.com/a> schema:lastModified \"-13798000000-01-01T00:00:00\"^^xsd:dateTime .\n" +
            "<http://test.com/b> schema:lastModified \"0000-01-01T00:00:00\"^^xsd:dateTime .\n" +
            "} WHERE {}";
        rdfRepository.update(query);

        TupleQueryResult results = rdfRepository.query("prefix schema: <http://schema.org/>\n" +
            "SELECT (?a - ?b as ?diff) WHERE { <http://test.com/a> schema:lastModified ?a . <http://test.com/b> schema:lastModified ?b } ORDER BY DESC(?diff)");
        BindingSet result = results.next();
        assertThat(result, binds("diff", new LiteralImpl("366.0", XMLSchema.DOUBLE)));

        result = results.next();
        assertThat(result, binds("diff", new LiteralImpl("-365.0", XMLSchema.DOUBLE)));

        result = results.next();
        assertThat(result, binds("diff", new LiteralImpl("-5.039616015E12", XMLSchema.DOUBLE)));
    }

    @Test
    public void dateFunctions() throws QueryEvaluationException {
        List<Statement> statements = new ArrayList<>();
        statements.add(statement("Q23", "P569", new LiteralImpl("0000-01-01T00:00:00Z", XMLSchema.DATETIME)));
        rdfRepository.sync("Q23", statements);
        TupleQueryResult results = rdfRepository.query("SELECT (year(?date) as ?year) WHERE { ?s ?p ?date }");
        BindingSet result = results.next();
        assertThat(result, binds("year", new LiteralImpl("0", XMLSchema.INTEGER)));
        results = rdfRepository.query("SELECT (day(?date) as ?day) WHERE { ?s ?p ?date FILTER (year(?date) != year(now())) }");
        result = results.next();
        assertThat(result, binds("day", new LiteralImpl("1", XMLSchema.INTEGER)));
    }

    @Test
    public void dateFunctionsMore() throws QueryEvaluationException {
        List<Statement> statements = new ArrayList<>();
        statements.add(statement("Q23", "P569", new LiteralImpl("0000-01-02T03:04:05Z", XMLSchema.DATETIME)));
        rdfRepository.sync("Q23", statements);
        TupleQueryResult results = rdfRepository.query("SELECT " +
            "(year(?date) as ?year) " +
            "(month(?date) as ?month) " +
            "(day(?date) as ?day) " +
            "(hours(?date) as ?hour) " +
            "(minutes(?date) as ?min) " +
            "(seconds(?date) as ?sec) " +
            " WHERE { ?s ?p ?date }");
        BindingSet result = results.next();
        assertThat(result, binds("year", new LiteralImpl("0", XMLSchema.INTEGER)));
        assertThat(result, binds("month", new LiteralImpl("1", XMLSchema.INTEGER)));
        assertThat(result, binds("day", new LiteralImpl("2", XMLSchema.INTEGER)));
        assertThat(result, binds("hour", new LiteralImpl("3", XMLSchema.INTEGER)));
        assertThat(result, binds("min", new LiteralImpl("4", XMLSchema.INTEGER)));
        assertThat(result, binds("sec", new LiteralImpl("5", XMLSchema.INTEGER)));
    }

    @Test
    public void dateNow() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query("SELECT (year(now()) as ?year) WHERE {  }");
        BindingSet result = results.next();
        int year = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT).get(Calendar.YEAR);
        assertThat(result, binds("year", new LiteralImpl(String.valueOf(year), XMLSchema.INTEGER)));
    }

}
