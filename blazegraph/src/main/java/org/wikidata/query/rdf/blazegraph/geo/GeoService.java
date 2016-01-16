package org.wikidata.query.rdf.blazegraph.geo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.jetty.client.HttpClient;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.blazegraph.inline.literal.WKTSerializer;
import org.wikidata.query.rdf.common.uri.Ontology;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.GeoSpatialServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.BD;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.service.geospatial.GeoSpatialSearchException;

/**
 * Implements a service to do geospatial search.
 * Base class for geospatial search wrappers.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public abstract class GeoService extends AbstractServiceFactory {

    /**
     * Delegate Blazegraph geosearch service.
     */
    private final GeoSpatialServiceFactory blazegraphService;

    /**
     * wikibase:globe parameter name.
     */
    public static final URIImpl GLOBE_PARAM = new URIImpl(
            Ontology.NAMESPACE + "globe");

    public GeoService() {
        super();
        blazegraphService = new GeoSpatialServiceFactory();
    }

    /**
     * Register the service so it is recognized by Blazegraph.
     */
    public static void register() {
        ServiceRegistry.getInstance().add(GeoAroundService.SERVICE_KEY, new GeoAroundService());
        ServiceRegistry.getInstance().add(GeoBoxService.SERVICE_KEY, new GeoBoxService());
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return blazegraphService.getServiceOptions();
    }

    /**
     * Get service parameter by name.
     *
     * @param serviceParams
     * @param paramName
     * @return
     */
    protected TermNode getParam(ServiceParams serviceParams, URI paramName) {
        TermNode node = serviceParams.get(paramName, null);
        if (node == null) {
            throw new IllegalArgumentException("Parameter " + paramName
                    + " is required.");
        }
        return node;
    }

    /**
     * Create service parameters for delegate service call.
     * @param params
     * @param serviceParams
     * @return
     */
    protected abstract JoinGroupNode buildServiceNode(ServiceCallCreateParams params,
            ServiceParams serviceParams);

    /**
     * Create globe node with appropriate value for coordSystem.
     * @param vf
     * @param serviceParams
     * @return
     */
    protected TermNode getGlobeNode(BigdataValueFactory vf, ServiceParams serviceParams) {
        final TermNode globeNode = serviceParams.get(GLOBE_PARAM, null);
        if (globeNode == null) {
            return new DummyConstantNode(vf.createLiteral(WKTSerializer.NO_GLOBE));
        }
        if (!globeNode.isConstant()) {
            // FIXME: add support for this
            throw new IllegalArgumentException("Non-constant globe value is not supported yet.");
        }
        BigdataValue v = globeNode.getValue();
        if (v instanceof BigdataURI) {
            WKTSerializer ser = new WKTSerializer();
            try {
                return new DummyConstantNode(vf.createLiteral(ser.trimCoordURI(v.stringValue())));
            } catch (GeoSpatialSearchException e) {
                // Unexpectedly wrong URI - still pass it along
                return globeNode;
            }
        }
        return globeNode;
    }

    /**
     * Extract pattern node from parameters.
     *
     * Pattern node looks like:
     *  ?place wdt:P625 ?location .
     * Both variables would be bound by the service.
     * @param params
     * @return
     */
    protected StatementPatternNode getPatternNode(ServiceCallCreateParams params) {
        ServiceNode serviceNode = params.getServiceNode();
        if (serviceNode == null)
            throw new IllegalArgumentException();

        List<StatementPatternNode> patterns = getStatementPatterns(serviceNode);

        if (patterns.size() == 0) {
            throw new IllegalArgumentException("This service requires arguments");
        }

        StatementPatternNode pattern = patterns.get(0);

        if (pattern == null) {
            throw new IllegalArgumentException();
        }

        if (!pattern.s().isVariable()) {
            throw new IllegalArgumentException(
                    "Search pattern subject must be a variable");
        }

        if (!pattern.p().isConstant()) {
            // FIXME: may be not necessary?
            throw new IllegalArgumentException(
                    "Search pattern predicate must be a constant");
        }

        if (!pattern.o().isVariable()) {
            throw new IllegalArgumentException(
                    "Search pattern object must be a variable");
        }

        return pattern;
    }

    @Override
    public BigdataServiceCall create(ServiceCallCreateParams params,
            ServiceParams serviceParams) {
        if (params == null)
            throw new IllegalArgumentException();

        final JoinGroupNode newGroup = buildServiceNode(params, serviceParams);
        final BigdataValueFactory vf = params.getTripleStore().getValueFactory();

        ServiceNode newServiceNode = new ServiceNode(
                new DummyConstantNode(vf.asValue(GeoSpatial.SEARCH)), newGroup);

        // Call delegate service
        HttpClient client = params.getClientConnectionManager();
        return (BigdataServiceCall) ServiceRegistry.getInstance().toServiceCall(
                params.getTripleStore(), client,
                GeoSpatial.SEARCH, newServiceNode, params.getStats());
    }

    /**
     * Returns the statement patterns contained in the service node.
     *
     * @param serviceNode
     * @return
     */
    protected List<StatementPatternNode> getStatementPatterns(final ServiceNode serviceNode) {

        final List<StatementPatternNode> statementPatterns =
                new ArrayList<StatementPatternNode>();

        for (IGroupMemberNode child : serviceNode.getGraphPattern()) {

            if (child instanceof StatementPatternNode) {
                statementPatterns.add((StatementPatternNode)child);
            } else {
                throw new GeoSpatialSearchException("Nested groups are not allowed.");
            }
        }

        return statementPatterns;
    }

    @Override
    public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {
        /**
         * This method extracts exactly those variables that are incoming,
         * i.e. must be bound before executing the execution of the service.
         *
         * Those can be only in service parameters.
         */
        final Set<IVariable<?>> requiredBound = new HashSet<IVariable<?>>();
        for (StatementPatternNode sp : getStatementPatterns(serviceNode)) {

            final TermNode subj = sp.s();
            final IVariableOrConstant<?> object = sp.o().getValueExpression();

            if (subj.isConstant() && BD.SERVICE_PARAM.equals(subj.getValue())) {
                if (object instanceof IVariable<?>) {
                    requiredBound.add((IVariable<?>)object); // the subject var is what we return
                }
            }
        }

        return requiredBound;
    }
}
