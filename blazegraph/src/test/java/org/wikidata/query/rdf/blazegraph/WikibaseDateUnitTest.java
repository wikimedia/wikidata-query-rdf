package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.test.Matchers.notBinds;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataStatement;

public class WikibaseDateUnitTest extends AbstractRandomizedBlazegraphTestBase {

    @Test
    public void dateExtension() {
        BigdataStatement statement = roundTrip(Ontology.Time.VALUE, Ontology.Time.VALUE,
                new LiteralImpl("-0101-01-01T00:00:00", XMLSchema.DATE));
        assertThat(statement.getObject().getIV(), instanceOf(LiteralExtensionIV.class));
    }

    @Test
    public void dateExtentsionQuery() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT * WHERE {BIND ( \"0001-01-01T00:00:00\"^^xsd:dateTime - \"-0001-01-01T00:00:00\"^^xsd:dateTime AS ?date)}");
        BindingSet result = results.next();
        // 731 days or 2 years since XML 1.1 has year 0 (which is 1BCE)
        assertThat(result, binds("date", new LiteralImpl("731.0", XMLSchema.DOUBLE)));
    }

    @Test
    public void dateFunctionQuery() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT * WHERE {BIND ( year(\"0000-01-01T00:00:00\"^^xsd:dateTime) AS ?date)}");
        BindingSet result = results.next();
        assertThat(result, binds("date", new LiteralImpl("0", XMLSchema.INTEGER)));
    }

    @Test
    public void dateFunctionQueryYear() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT * WHERE {BIND ( year(\"-13798000000-01-01T00:00:00\"^^xsd:dateTime) AS ?date)}");
        BindingSet result = results.next();
        assertThat(result, binds("date", new LiteralImpl("-13798000000", XMLSchema.INTEGER)));
    }

    @Test
    public void datePlusPeriod() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT ?now WHERE { BIND( ( \"2016-01-01T00:00:00\"^^xsd:dateTime + \"P7D\"^^xsd:duration ) AS ?now ) . }");
        BindingSet result = results.next();
        assertThat(result, binds("now", new LiteralImpl("2016-01-08T00:00:00Z", XMLSchema.DATETIME)));
    }

    @Test
    public void dateMinusPeriod() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT ?now WHERE { BIND( ( \"2016-01-01T00:00:00\"^^xsd:dateTime - \"P7D\"^^xsd:duration ) AS ?now ) . }");
        BindingSet result = results.next();
        assertThat(result, binds("now", new LiteralImpl("2015-12-25T00:00:00Z", XMLSchema.DATETIME)));
    }

    @Test
    public void dateAndString() throws QueryEvaluationException {
        // See https://phabricator.wikimedia.org/T140151
        TupleQueryResult results = query("SELECT ?age WHERE { "
                + "BIND(\"2016-05-13T15:02:21Z\"^^xsd:dateTime as ?dateOfBirth) \n"
                + "BIND(\"+0000-03-13T00:00:00Z\" as ?dateOfDeath) \n"
                + "BIND(?dateOfDeath - ?dateOfBirth AS ?age). }");
        BindingSet result = results.next();
        assertThat(result, notBinds("age"));
    }

    // TODO: @Test - does not work yet
    public void dateCompare() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT ((\"1990-01-01\"^^xsd:date < now()) as ?answer) WHERE {  }");
        BindingSet result = results.next();
        assertThat(result, binds("answer", new LiteralImpl("true", XMLSchema.BOOLEAN)));
    }
}
