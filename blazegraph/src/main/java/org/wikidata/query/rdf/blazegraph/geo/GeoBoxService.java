package org.wikidata.query.rdf.blazegraph.geo;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatial.GeoFunction;

import cutthecrap.utils.striterators.ICloseableIterator;

import static org.wikidata.query.rdf.blazegraph.geo.GeoUtils.pointFromIV;

/**
 * Implements a service to do geospatial search.
 *
 * This class searches for items around certain point.
 *
 * Example:
 *
 * SELECT * WHERE {
 *   wd:Q90 wdt:P625 ?parisLoc .
 *   SERVICE wikibase:box {
 *      ?place wdt:P625 ?location .
 *      bd:serviceParam wikibase:cornerSouthWest "Point(48.0 2.0)"^^ogc:wktLiteral .
 *      bd:serviceParam wikibase:cornerNorthEast "Point(49.0 3.0)"^^ogc:wktLiteral .
 *   }
 * }
 *
 *   Or:
 *
 * SELECT * WHERE {
 *   wd:Q90 wdt:P625 ?parisLoc .
 *   SERVICE wikibase:box {
 *      ?place wdt:P625 ?location .
 *      bd:serviceParam wikibase:cornerEast "Point(49.0 3.0)"^^ogc:wktLiteral .
 *      bd:serviceParam wikibase:cornerWest "Point(48.0 2.0)"^^ogc:wktLiteral .
 *   }
 * }
 *
 * The latter form automatically assigns point to the north to be NE corner and point
 * to the south to be SW corner.
 */
public class GeoBoxService extends GeoService {

    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(
            Ontology.NAMESPACE + "box");

    /**
     * wikibase:cornerNorthEast parameter name.
     */
    public static final URIImpl NE_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerNorthEast");

    /**
     * wikibase:cornerSouthWest parameter name.
     */
    public static final URIImpl SW_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerSouthWest");

    /**
     * wikibase:cornerEast parameter name.
     */
    public static final URIImpl EAST_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerEast");

    /**
     * wikibase:cornerWest parameter name.
     */
    public static final URIImpl WEST_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerWest");

    /**
     * Annotation for node. Will contain the replacement variable node.
     */
    public static final String VAR_ANNOTATION = GeoBoxService.class.getName()
            + ".var";
    /**
     * Service param to specify we need coordinate wrapping.
     */
    public static final URIImpl WRAP_PARAM = new URIImpl(
            Ontology.NAMESPACE + "cornerWrap");

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
        // ?var geo:search "inRectangle" .
        newGroup.addArg(new StatementPatternNode(searchVar,
                new DummyConstantNode(vf.asValue(GeoSpatial.SEARCH)),
                new DummyConstantNode(vf
                        .createLiteral(GeoFunction.IN_RECTANGLE.toString()))));
        // ?var geo:predicate wdt:P625 .
        newGroup.addArg(new StatementPatternNode(searchVar,
                new DummyConstantNode(vf.asValue(GeoSpatial.PREDICATE)),
                predicate));
        // ?var geo:searchDatatype ogc:wktLiteral .
        newGroup.addArg(new StatementPatternNode(searchVar,
                new DummyConstantNode(vf.asValue(GeoSpatial.SEARCH_DATATYPE)),
                new ConstantNode(
                        voc.getConstant(new URIImpl(GeoSparql.WKT_LITERAL)))));
        if (serviceParams.contains(NE_PARAM)) {
            // ?var geo:spatialRectangleNorthEast ?ne .
            newGroup.addArg(new StatementPatternNode(searchVar,
                    new DummyConstantNode(vf
                            .asValue(GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST)),
                    getParam(serviceParams, NE_PARAM)));
            // ?var geo:spatialRectangleNorthEast ?sw .
            newGroup.addArg(new StatementPatternNode(searchVar,
                    new DummyConstantNode(vf
                            .asValue(GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)),
                    getParam(serviceParams, SW_PARAM)));
        } else if (serviceParams.contains(EAST_PARAM)) {
            final TermNode east = getParam(serviceParams, EAST_PARAM);
            final TermNode west = getParam(serviceParams, WEST_PARAM);

            if (east instanceof ConstantNode && west instanceof ConstantNode) {
                // Easy case - both constants
                final WikibasePoint eastWP = pointFromIV(
                        ((ConstantNode) east).getValue().getIV());
                final WikibasePoint westWP = pointFromIV(
                        ((ConstantNode) west).getValue().getIV());

                final GeoUtils.Box box = new GeoUtils.Box(eastWP, westWP);
                TermNode ne;
                TermNode sw;
                if (box.switched()) {
                    ne = new DummyConstantNode(vf.asValue(
                            vf.createLiteral(box.northEast().toString(),
                                    new URIImpl(GeoSparql.WKT_LITERAL))));
                    sw = new DummyConstantNode(vf.asValue(
                            vf.createLiteral(box.southWest().toString(),
                                    new URIImpl(GeoSparql.WKT_LITERAL))));
                } else {
                    ne = east;
                    sw = west;
                }
                // ?var geo:spatialRectangleNorthEast ?ne .
                newGroup.addArg(
                        new StatementPatternNode(searchVar,
                                new DummyConstantNode(vf.asValue(
                                        GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST)),
                        ne));
                // ?var geo:spatialRectangleNorthEast ?sw .
                newGroup.addArg(
                        new StatementPatternNode(searchVar,
                                new DummyConstantNode(vf.asValue(
                                        GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)),
                        sw));
            } else {
                // Hard case - non-constants
                // Add dummy var to the node
                serviceParams.add(WRAP_PARAM, VarNode.freshVarNode());
                // ?var geo:spatialRectangleNorthEast ?ne .
                newGroup.addArg(new StatementPatternNode(searchVar,
                        new DummyConstantNode(vf.asValue(
                                GeoSpatial.SPATIAL_RECTANGLE_NORTH_EAST)),
                        getSubstituteVar(east)));
                // ?var geo:spatialRectangleNorthEast ?sw .
                newGroup.addArg(new StatementPatternNode(searchVar,
                        new DummyConstantNode(vf.asValue(
                                GeoSpatial.SPATIAL_RECTANGLE_SOUTH_WEST)),
                        getSubstituteVar(west)));
            }
        } else {
            throw new IllegalArgumentException(
                    "Box corner parameters are required");
        }
        // ?var geo:locationValue ?location .
        newGroup.addArg(new StatementPatternNode(searchVar,
                new DummyConstantNode(vf.asValue(GeoSpatial.LOCATION_VALUE)),
                locationVar));
        // ?var geo:coordSystem "2" .
        newGroup.addArg(new StatementPatternNode(searchVar,
                new DummyConstantNode(vf.asValue(GeoSpatial.COORD_SYSTEM)),
                getGlobeNode(vf, serviceParams)));

        return newGroup;
    }

    /**
     * Generate substitute node for the term.
     *
     * @param term
     *            Term to substitute
     * @return
     */
    private TermNode getSubstituteVar(TermNode term) {
        if (term.isVariable()) {
            if (((IVariable<?>) term.getValueExpression()).isAnonymous()) {
                throw new IllegalArgumentException(
                        "Anonymous vars not supported as box corners");
            }
        }
        final VarNode newnode = VarNode.freshVarNode();
        term.setProperty(VAR_ANNOTATION, newnode);
        return newnode;
    }

    @Override
    public BigdataServiceCall create(ServiceCallCreateParams params,
            ServiceParams serviceParams) {
        final BigdataServiceCall parentCall = super.create(params,
                serviceParams);
        if (serviceParams.contains(WRAP_PARAM)) {
            return new GeoBoxServiceCall(parentCall,
                    getParam(serviceParams, EAST_PARAM),
                    getParam(serviceParams, WEST_PARAM),
                    params.getTripleStore());
        }
        return parentCall;
    }

    /**
     * Service call wrapper to switch coordinates for box search.
     */
    @SuppressWarnings("rawtypes")
    private class GeoBoxServiceCall implements BigdataServiceCall {

        /**
         * Actual GeoService call.
         */
        private final BigdataServiceCall wrappedCall;
        /**
         * East corner term.
         */
        private final TermNode east;
        /**
         * West corner term.
         */
        private final TermNode west;
        /**
         * KB store.
         */
        private final AbstractTripleStore kb;
        /**
         * Value factory.
         */
        private final BigdataValueFactory vf;

        GeoBoxServiceCall(BigdataServiceCall wrappedCall, TermNode east,
                TermNode west, AbstractTripleStore kb) {
            this.wrappedCall = wrappedCall;
            this.east = east;
            this.west = west;
            this.kb = kb;
            this.vf = kb.getValueFactory();
        }

        @Override
        public IServiceOptions getServiceOptions() {
            return wrappedCall.getServiceOptions();
        }

        @Override
        public ICloseableIterator<IBindingSet> call(IBindingSet[] bindingSets)
                throws Exception {
            for (IBindingSet bs : bindingSets) {
                updateBindingSet(bs);
            }
            return wrappedCall.call(bindingSets);
        }

        /**
         * Get temp variable associated with this term.
         *
         * @param term
         * @return The variable.
         */
        private IVariable<IV> getAssociatedVariable(TermNode term) {
            final VarNode node = (VarNode) term.getProperty(VAR_ANNOTATION);
            return node.getValueExpression();
        }

        /**
         * Update binding set with suitable definitions of temp vars.
         *
         * @param bs
         *            Binding set
         */
        private void updateBindingSet(IBindingSet bs) {
            final IV eastIV = east.getValueExpression().get(bs);
            final IV westIV = west.getValueExpression().get(bs);

            final GeoUtils.Box box = new GeoUtils.Box(pointFromIV(eastIV),
                    pointFromIV(westIV));

            bs.set(getAssociatedVariable(east),
                    new Constant<IV>(createIV(box.northEast())));
            bs.set(getAssociatedVariable(west),
                    new Constant<IV>(createIV(box.southWest())));
        }

        /**
         * Create new mock IV for a wikibase point.
         *
         * @param wp
         * @return
         */
        private IV createIV(WikibasePoint wp) {
            final BigdataLiteral literal = vf.createLiteral(wp.toString(),
                    new URIImpl(GeoSparql.WKT_LITERAL));
            TermId mock = TermId.mockIV(VTE.LITERAL);
            mock.setValue(vf.asValue(literal));
            return mock;
        }
    }
}
