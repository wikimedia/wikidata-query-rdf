package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.instanceOf;

import java.math.BigInteger;

import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataStatement;

public class WikibaseGeoUnitTest extends AbstractRandomizedBlazegraphTestBase {
    @Test
    public void geoExtension() {
        BigdataStatement statement = roundTrip(Ontology.Geo.GLOBE, Ontology.Geo.LATITUDE,
                new LiteralImpl("Point(1.2 3.4)", new URIImpl(GeoSparql.WKT_LITERAL)));
        assertThat(statement.getObject().getIV(), instanceOf(LiteralExtensionIV.class));
        assertEquals(statement.getObject().toString(),
                "\"Point(1.2 3.4)\"^^<" + GeoSparql.WKT_LITERAL + ">");
    }

    @Test
    public void geoExtensionGlobe() {
        String point = "<" + uris().entity() + "Q1234> Point(5.6 7.8)";
        BigdataStatement statement = roundTrip(Ontology.Geo.GLOBE, Ontology.Geo.LATITUDE,
                new LiteralImpl(point, new URIImpl(GeoSparql.WKT_LITERAL)));
        assertThat(statement.getObject().getIV(), instanceOf(LiteralExtensionIV.class));
        assertEquals(statement.getObject().toString(),
                "\"" + point + "\"^^<" + GeoSparql.WKT_LITERAL + ">");
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testInteger() {
        LexiconRelation rel = store().getLexiconRelation();
        Literal l = new LiteralImpl("Point(40.4426 -80.0068)", new URIImpl(GeoSparql.WKT_LITERAL));
        LiteralExtensionIV iv = (LiteralExtensionIV)rel.getInlineIV(l);
        assertEquals(GeoSparql.WKT_LITERAL, iv.getExtensionIV().getValue().toString());
        assertEquals(new XSDIntegerIV(new BigInteger("1008819921758573694187894119050371595694851885687493623808")), iv.getDelegate());
    }
}
