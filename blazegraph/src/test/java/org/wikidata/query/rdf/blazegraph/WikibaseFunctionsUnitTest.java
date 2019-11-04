package org.wikidata.query.rdf.blazegraph;

import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.test.Matchers.binds;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class WikibaseFunctionsUnitTest extends AbstractBlazegraphTestBase {

    @Test
    public void urlDecodeQuery() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT * WHERE {\n"
                + "BIND ( wikibase:decodeUri(\"this%20is%20a%20test%20%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82%21\") AS ?result)\n"
                + "}");
        BindingSet result = results.next();
        assertThat(result, binds("result", new LiteralImpl("this is a test привет!")));
    }

    @Test
    public void urlDecodeQueryRoundtrip() throws QueryEvaluationException {
        TupleQueryResult results = query("SELECT * WHERE {\n"
                + "BIND ( encode_for_uri(\"encoding test - []&5%<>?., проверка кодирования\") AS ?encoded)\n"
                + "BIND ( wikibase:decodeUri(?encoded) AS ?result)\n"
                + "}");
        BindingSet result = results.next();
        assertThat(result, binds("result", new LiteralImpl("encoding test - []&5%<>?., проверка кодирования")));
    }
}
