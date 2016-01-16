package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.test.Matchers.binds;

import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

public class WikibaseGeoExtensionIntegrationTest extends AbstractUpdateIntegrationTestBase {

    private final String moonURI = uris().entity() + "Q405";

    private void insertPoints() {
        String query = "INSERT {\n" +
                "<http://Berlin> wdt:P625 \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                "<http://Bremen> wdt:P625 \"Point(8.808888 53.07694)\"^^geo:wktLiteral .\n" +
                "<http://Barcelona> wdt:P625 \"Point(2.17694 41.3825)\"^^geo:wktLiteral .\n" +
                "<http://SanFrancisco> wdt:P625 \"Point(-122.43333 37.76666)\"^^geo:wktLiteral .\n" +
                "<http://Johannesburg> wdt:P625 \"Point(2.77777 -26.145 )\"^^geo:wktLiteral .\n" +
                "<http://MoonBerlin> wdt:P625 \"<" + moonURI + "> Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                "<http://MoonBremen> wdt:P625 \"<" + moonURI + "> Point(8.808888 53.07694)\"^^geo:wktLiteral .\n" +
                "} WHERE {}";
        rdfRepository().update(query);
    }

    private void resultsAre(TupleQueryResult results, String var, String... matches) throws QueryEvaluationException {
        BindingSet result;
        for (String match : matches) {
            result = results.next();
            assertThat(result, binds(var, new URIImpl(match)));
        }
        assertFalse(results.hasNext());
    }

    @Test
    public void circleSearch() throws QueryEvaluationException {
        insertPoints();

        TupleQueryResult results = rdfRepository().query(
                "SELECT * WHERE {\n" +
                "SERVICE wikibase:around {\n" +
                "  ?place wdt:P625 ?location .\n" +
                " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n "
        );

        resultsAre(results, "place", "http://Berlin", "http://Bremen");

        results = rdfRepository().query(
                "SELECT * WHERE {\n" +
                "SERVICE wikibase:around {\n" +
                "  ?place wdt:P625 ?location .\n" +
                " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                " bd:serviceParam wikibase:radius \"2000\" .}} ORDER BY ?place\n "
        );

        resultsAre(results, "place", "http://Barcelona", "http://Berlin", "http://Bremen");
    }

    @Test
    public void circleSearchGlobe() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository().query(
                "SELECT * WHERE {\n" +
                "SERVICE wikibase:around {\n" +
                "  ?place wdt:P625 ?location .\n" +
                " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                " bd:serviceParam wikibase:globe <" + moonURI + ">.\n " +
                " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n "
        );

        resultsAre(results, "place", "http://MoonBerlin", "http://MoonBremen");
    }

    @Test
    public void circleSearchGlobeNumber() throws QueryEvaluationException {
        insertPoints();

        TupleQueryResult results = rdfRepository().query(
                "SELECT * WHERE {\n" +
                "SERVICE wikibase:around {\n" +
                "  ?place wdt:P625 ?location .\n" +
                " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n" +
                " bd:serviceParam wikibase:globe \"405\" .\n " +
                " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n "
        );

        resultsAre(results, "place", "http://MoonBerlin", "http://MoonBremen");
    }

    @Test
    public void boxSearch() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository().query(
                "SELECT * WHERE {\n" +
                "SERVICE wikibase:box {\n" +
                "  ?place wdt:P625 ?location .\n" +
                " bd:serviceParam wikibase:cornerSouthWest \"Point(-180 -50)\"^^geo:wktLiteral .\n" +
                " bd:serviceParam wikibase:cornerNorthEast \"Point(180 50)\"^^geo:wktLiteral .\n" +
                " }} ORDER BY ?place\n "
        );

        resultsAre(results, "place", "http://Barcelona", "http://Johannesburg", "http://SanFrancisco");
    }

}
