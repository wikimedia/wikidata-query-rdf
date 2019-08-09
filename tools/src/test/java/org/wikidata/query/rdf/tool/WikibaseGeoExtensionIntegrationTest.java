package org.wikidata.query.rdf.tool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.test.Matchers.binds;

import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

public class WikibaseGeoExtensionIntegrationTest extends AbstractUpdaterIntegrationTestBase {

    private final String moonURI = UrisSchemeFactory.getURISystem().entityIdToURI("Q405");

    private void insertPoints() {
        String query = "INSERT {\n"
                + "<http://Berlin> wdt:P625 \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + "<http://Bremen> wdt:P625 \"Point(8.808888 53.07694)\"^^geo:wktLiteral .\n"
                + "<http://Barcelona> wdt:P625 \"Point(2.17694 41.3825)\"^^geo:wktLiteral .\n"
                + "<http://SanFrancisco> wdt:P625 \"Point(-122.43333 37.76666)\"^^geo:wktLiteral .\n"
                + "<http://Johannesburg> wdt:P625 \"Point(2.77777 -26.145 )\"^^geo:wktLiteral .\n"
                + "<http://MoonBerlin> wdt:P625 \"<" + moonURI + "> Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + "<http://MoonBremen> wdt:P625 \"<" + moonURI + "> Point(8.808888 53.07694)\"^^geo:wktLiteral .\n"
                + "} WHERE {}";
        rdfRepository.update(query);
    }

    private void resultsAreLiteral(TupleQueryResult results, String var,
            URI type, String... matches) throws QueryEvaluationException {
        BindingSet result;
        for (String match : matches) {
            result = results.next();
            assertThat(result, binds(var, new LiteralImpl(match, type)));
        }
        assertFalse(results.hasNext());
    }

    private void resultsAre(TupleQueryResult results, String var,
            String... matches) throws QueryEvaluationException {
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

        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:around {\n"
                + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Berlin", "http://Bremen");

        results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:around {\n"
                + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:radius \"2000\" .}} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Barcelona", "http://Berlin",
                "http://Bremen");
    }

    @Test
    public void circleSearchGlobe() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:around {\n"
                + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:globe <" + moonURI + ">.\n "
                + " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://MoonBerlin", "http://MoonBremen");
    }

    @Test
    public void circleSearchGlobeNumber() throws QueryEvaluationException {
        insertPoints();

        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:around {\n"
                + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:globe \"405\" .\n "
                + " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://MoonBerlin", "http://MoonBremen");
    }

    @Test
    public void circleSearchWithDistance() throws QueryEvaluationException {
        insertPoints();

        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:around {\n"
                + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:center \"Point(13.38333 52.516666)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:distance ?distance .\n"
                + " bd:serviceParam wikibase:radius \"320\" .}} ORDER BY ?place\n ");

        resultsAreLiteral(results, "distance", XMLSchema.DOUBLE, "0.0",
                "313.728");
    }

    @Test
    public void boxSearch() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:box {\n" + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:cornerSouthWest \"Point(-150 -50)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:cornerNorthEast \"Point(150 50)\"^^geo:wktLiteral .\n"
                + " }} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Barcelona", "http://Johannesburg",
                "http://SanFrancisco");
    }

    @Test
    public void boxSearchCornersConst() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + "SERVICE wikibase:box {\n" + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:cornerWest \"Point(-150 -50)\"^^geo:wktLiteral .\n"
                + " bd:serviceParam wikibase:cornerEast \"Point(150 50)\"^^geo:wktLiteral .\n"
                + " }} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Barcelona", "http://Johannesburg",
                "http://SanFrancisco");
    }

    @Test
    public void boxSearchCornersVar() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + " BIND(\"Point(-150 -50)\"^^geo:wktLiteral as ?west) \n"
                + " BIND(\"Point(150 50)\"^^geo:wktLiteral as ?east) \n"
                + "SERVICE wikibase:box {\n" + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:cornerWest ?west .\n"
                + " bd:serviceParam wikibase:cornerEast ?east .\n"
                + " }} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Barcelona", "http://Johannesburg",
                "http://SanFrancisco");
    }

    @Test
    public void boxSearchCornersVarSwitch() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {\n"
                + " BIND(\"Point(-150 50)\"^^geo:wktLiteral as ?west) \n"
                + " BIND(\"Point(150 -50)\"^^geo:wktLiteral as ?east) \n"
                + "SERVICE wikibase:box {\n" + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:cornerWest ?west .\n"
                + " bd:serviceParam wikibase:cornerEast ?east .\n"
                + " }} ORDER BY ?place\n ");

        resultsAre(results, "place", "http://Barcelona", "http://Johannesburg",
                "http://SanFrancisco");
    }

    @Test
    public void distance() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query(
                "SELECT * WHERE {BIND ( geof:distance(\"Point(0 0)\"^^geo:wktLiteral, \"Point(-1 -1)\"^^geo:wktLiteral) AS ?distance)}");
        BindingSet result = results.next();
        // distance between two points
        assertThat(result, binds("distance",
                new LiteralImpl("157.2418158675294", XMLSchema.DOUBLE)));
    }

    @Test
    public void distanceSelf() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {"
                + "BIND(\"Point(-81.4167 -80.0)\"^^geo:wktLiteral as ?point)\n"
                + "BIND(geof:distance(?point, ?point) AS ?distance)}");
        BindingSet result = results.next();
        // distance between point and itself should be 0
        assertThat(result,
                binds("distance", new LiteralImpl("0.0", XMLSchema.DOUBLE)));
    }

    @Test
    public void distanceWithUnits() throws QueryEvaluationException {
        // FIXME: for now, we just accept anything as units and ignore it. We
        // need to do better.
        TupleQueryResult results = rdfRepository.query(
                "SELECT * WHERE {BIND ( geof:distance(\"Point(0 0)\"^^geo:wktLiteral, \"Point(-1 -1)\"^^geo:wktLiteral, wd:Q2) AS ?distance)}");
        BindingSet result = results.next();
        // distance between two points
        assertThat(result, binds("distance",
                new LiteralImpl("157.2418158675294", XMLSchema.DOUBLE)));
    }

    @Test
    public void coordinatePartsNoGlobe() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {"
                + "BIND(\"Point(-81.4167 -80.01)\"^^geo:wktLiteral as ?point)\n"
                + "BIND(geof:globe(?point) AS ?globe)\n"
                + "BIND(geof:longitude(?point) AS ?longitude)\n"
                + "BIND(geof:latitude(?point) AS ?latitude)\n"
                + "}");
        BindingSet result = results.next();
        assertThat(result, binds("longitude",
                new LiteralImpl("-81.4167", XMLSchema.DOUBLE)));
        assertThat(result, binds("latitude",
                new LiteralImpl("-80.01", XMLSchema.DOUBLE)));
        assertThat(result, binds("globe",
                new LiteralImpl("")));
    }

    @Test
    public void coordinatePartsWithGlobe() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {"
                + "BIND(\"<" + moonURI + "> Point(81.67 17.42)\"^^geo:wktLiteral as ?point)\n"
                + "BIND(geof:globe(?point) AS ?globe)\n"
                + "BIND(geof:longitude(?point) AS ?longitude)\n"
                + "BIND(geof:latitude(?point) AS ?latitude)\n"
                + "}");
        BindingSet result = results.next();
        assertThat(result, binds("longitude",
                new LiteralImpl("81.67", XMLSchema.DOUBLE)));
        assertThat(result, binds("latitude",
                new LiteralImpl("17.42", XMLSchema.DOUBLE)));
        assertThat(result, binds("globe", new URIImpl(moonURI)));
    }

    // Check distance between close points
    @Test
    public void distanceClosePoints() throws QueryEvaluationException {
        TupleQueryResult results = rdfRepository.query("SELECT * WHERE {"
                + "BIND(\"Point(-96.0775581 46.2830152)\"^^geo:wktLiteral as ?b1)\n"
                + "BIND(\"Point(-96.077558 46.283015)\"^^geo:wktLiteral as ?b2)\n"
                + "BIND(geof:distance(?b1, ?b2) as ?distance)\n"
                + "}");
        BindingSet result = results.next();
        assertThat(result, binds("distance",
                new LiteralImpl("0.0", XMLSchema.DOUBLE)));
    }
}
