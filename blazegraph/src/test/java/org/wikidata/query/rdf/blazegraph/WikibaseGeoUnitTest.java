package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.blazegraph.geo.GeoUtils.pointLiteral;
import static org.wikidata.query.rdf.test.Matchers.binds;

import java.math.BigInteger;

import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.blazegraph.geo.GeoUtils;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataStatement;

public class WikibaseGeoUnitTest extends AbstractBlazegraphTestBase {
    @Test
    public void geoExtension() {
        BigdataStatement statement = roundTrip(Ontology.Geo.GLOBE, Ontology.Geo.LATITUDE,
                pointLiteral("Point(1.2 3.4)"));
        assertThat(statement.getObject().getIV(), instanceOf(LiteralExtensionIV.class));
        assertEquals(statement.getObject().toString(),
                "\"Point(1.2 3.4)\"^^<" + GeoSparql.WKT_LITERAL + ">");
    }

    @Test
    public void geoExtensionGlobe() {
        String point = "<" + uris().entityIdToURI("Q1234") + "> Point(5.6 7.8)";
        BigdataStatement statement = roundTrip(Ontology.Geo.GLOBE, Ontology.Geo.LATITUDE,
                pointLiteral(point));
        assertThat(statement.getObject().getIV(), instanceOf(LiteralExtensionIV.class));
        assertEquals(statement.getObject().toString(),
                "\"" + point + "\"^^<" + GeoSparql.WKT_LITERAL + ">");
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testInteger() {
        LexiconRelation rel = store().getLexiconRelation();
        Literal l = pointLiteral("Point(40.4426 -80.0068)");
        LiteralExtensionIV iv = (LiteralExtensionIV)rel.getInlineIV(l);
        assertEquals(GeoSparql.WKT_LITERAL, iv.getExtensionIV().getValue().toString());
        assertEquals(new XSDIntegerIV(new BigInteger("1008819921758573694187894119050371595694851885687493623808")), iv.getDelegate());
    }

    private final String moonURI = uris().entityIdToURI("Q405");

    private void insertPoints() {
        add("http://Berlin", "wdt:P625",
                pointLiteral("Point(13.38333 52.516666)"));
        add("http://Bremen", "wdt:P625",
                pointLiteral("Point(8.808888 53.07694)"));
        add("http://Barcelona", "wdt:P625",
                pointLiteral("Point(2.17694 41.3825)"));
        add("http://SanFrancisco", "wdt:P625",
                pointLiteral("Point(-122.43333 37.76666)"));
        add("http://Johannesburg", "wdt:P625",
                pointLiteral("Point(2.77777 -26.145)"));
        add("http://MoonBerlin", "wdt:P625", pointLiteral(
                "<" + moonURI + "> " + "Point(13.38333 52.516666)"));
        add("http://MoonBremen", "wdt:P625", pointLiteral(
                "<" + moonURI + "> " + "Point(13.38333 52.516666)"));
    }

    @Test
    public void boxTest() {
        textBox("Point(150 50)", "Point(-150 -50)", "Point(150 50)", "Point(-150 -50)");
        textBox("Point(150 -50)", "Point(-150 50)", "Point(150 50)", "Point(-150 -50)");
    }

    private void textBox(String east, String west, String ne, String sw) {
        final GeoUtils.Box box = new  GeoUtils.Box(new WikibasePoint(east), new WikibasePoint(west));
        assertEquals(ne, box.northEast().toString());
        assertEquals(sw, box.southWest().toString());
        assertEquals(box.northEast().toString().equals(east), !box.switched());
    }

    // FIXME: does not work now, figure out how to make this work
    public void boxSearchQuery() throws QueryEvaluationException {
        insertPoints();
        TupleQueryResult results = query("SELECT * WHERE {\n"
                + " BIND(\"Point(-150 50)\"^^geo:wktLiteral as ?west) \n"
                + " BIND(\"Point(150 -50)\"^^geo:wktLiteral as ?east) \n"
                + "SERVICE wikibase:box {\n" + "  ?place wdt:P625 ?location .\n"
                + " bd:serviceParam wikibase:cornerWest ?west .\n"
                + " bd:serviceParam wikibase:cornerEast ?east .\n"
                + " }} ORDER BY ?place\n ");
        BindingSet result = results.next();
        assertThat(result, binds("place", new URIImpl("http://Barcelona")));
    }

}
