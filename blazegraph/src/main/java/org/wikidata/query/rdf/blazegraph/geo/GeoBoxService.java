package org.wikidata.query.rdf.blazegraph.geo;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;

/**
 * Implements a service to do geospatial search.
 *
 * This class searches for items around certain point.
 *
 * Example:
 *
 * SELECT * WHERE {
 *   wd:Q90 wdt:P625 ?parisLoc .
 *
 *   SERVICE wikibase:box {
 *     ?place wdt:P625 ?location .
 *     bd:serviceParam wikibase:cornerSouthWest "Point(48.0 2.0)"^^ogc:wktLiteral .
 *     bd:serviceParam wikibase:cornerNorthEast "Point(49.0 3.0)"^^ogc:wktLiteral .
 *   }
 * }
 */
public class GeoBoxService extends GeoService {

    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(
            Ontology.NAMESPACE + "box");

    /**
     * wikibase:center parameter name.
     */
    public static final URIImpl NE_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerNorthEast");

    /**
     * wikibase:radius parameter name.
     */
    public static final URIImpl SW_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerSouthWest");

    @Override
    protected JoinGroupNode buildServiceNode(ServiceCallCreateParams params,
            ServiceParams serviceParams) {
        final AbstractTripleStore store = params.getTripleStore();
        final Vocabulary voc = store.getVocabulary();
        BigdataValueFactory vf = store.getValueFactory();

        final StatementPatternNode pattern = getPatternNode(params);
        final TermNode searchVar = pattern.s();
        final TermNode predicate = pattern.p();
        final TermNode locationVar = pattern.o();

        final JoinGroupNode newGroup = new JoinGroupNode();
        // ?var geo:search "inCircle" .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.SEARCH)),
                        new DummyConstantNode(vf.createLiteral(GeoFunction.IN_RECTANGLE.toString()))
                ));
        // ?var geo:predicate wdt:P625 .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.PREDICATE)),
                        predicate
                ));
        // ?var geo:searchDatatype ogc:wktLiteral .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.SEARCH_DATATYPE)),
                        new ConstantNode(
                                voc.getConstant(new URIImpl(GeoSparql.WKT_LITERAL)))));

        // ?var geo:spatialCircleCenter ?parisLoc .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST)),
                        getParam(serviceParams, NE_PARAM)
                ));
        // ?var geo:spatialCircleRadius "1" .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)),
                        getParam(serviceParams, SW_PARAM)
                ));
        // ?var geo:locationValue ?location .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.LOCATION_VALUE)),
                        locationVar));
        // ?var geo:coordSystem "2" .
        newGroup.addArg(new StatementPatternNode(
                        searchVar,
                        new DummyConstantNode(vf.asValue(GeoSpatial.COORD_SYSTEM)),
                        getGlobeNode(vf, serviceParams)
                ));

        return newGroup;
    }
}
