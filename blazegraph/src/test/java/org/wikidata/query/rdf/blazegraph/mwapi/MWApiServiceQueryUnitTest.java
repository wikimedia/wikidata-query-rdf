package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.wikidata.query.rdf.test.Matchers.assertResult;
import static org.wikidata.query.rdf.test.Matchers.binds;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.AbstractBlazegraphTestBase;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.test.Randomizer;

import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;

public class MWApiServiceQueryUnitTest extends AbstractBlazegraphTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(MWApiServiceQueryUnitTest.class);

    @Rule
    public final Randomizer randomizer = new Randomizer();

    //    @Test // disabled test, need to be discussed it it is safe to run in in CI
    public void mwAPI_T168876() {
        // MWAPI service throws “could not find binding for parameter” if optimizer is not disabled
        // https://phabricator.wikimedia.org/T168876
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        add("wd:Q168876", "wdt:P18", new LiteralImpl("http://commons.wikimedia.org/wiki/Special:FilePath/U8%20Berlin%20Leinestrasse%2012-2017.jpg"));
        query.append("SELECT * WHERE {\n" +
                "  BIND(wd:Q168876 as ?leinestr) # random item with picture\n" +
                "  ?leinestr wdt:P18 ?picture.\n" +
                "\n" +
                "  BIND(STRAFTER(str(?picture), \"Special:FilePath/\") AS ?filename)\n" +
                "  BIND(ontology:decodeUri(CONCAT(\"File:\", ?filename)) AS ?file)\n" +
                "\n" +
                "  SERVICE ontology:mwapi {\n" +
                "    bd:serviceParam ontology:api \"Categories\".\n" +
                "    bd:serviceParam ontology:endpoint \"commons.wikipedia.org\".\n" +
                "    bd:serviceParam mwapi:titles ?file. # throws IllegalArgumentException: Could not find binding for parameter titles\n" +
                "    ?cat ontology:apiOutput mwapi:category.\n" +
                "  }\n" +
                "}");
        assertQueryResult(query.toString(),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class),
                binds("cat", Literal.class)
        );
    }

    // Blazegraph throws RuntimeException wrapping various checked exceptions and it make it very useful
    // to print the query plan for debugging if any test fails
    @SuppressWarnings({"checkstyle:illegalcatch"})
    @SafeVarargs
    private final void assertQueryResult(String query, final Matcher<BindingSet>... bindingMatchers) {
        ASTContainer astContainer = null;
        try {
            TupleQueryResult q;
            try {
                astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);
                q = ASTEvalHelper.evaluateTupleQuery(store(), astContainer, new QueryBindingSet(), null);
            } catch (MalformedQueryException | QueryEvaluationException e) {
                throw new RuntimeException(e);
            }
            assertResult(q, bindingMatchers);
        } catch (AssertionError | RuntimeException e) {
            LOG.error("Error while checking results for {} ", astContainer);
            throw e;
        }
    }
}
