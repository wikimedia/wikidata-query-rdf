package org.wikidata.query.rdf.blazegraph.mwapi;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Mediawiki;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.BD;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Service factory for calling out to Mediawiki API Services.
 * Service call looks like:
 *
 *  Fetching catrgories for "Albert Einstein" page:
 *
 *  SERVICE wikibase:mwapi {
 *      bd:serviceParam wikibase:api "Categories" .
 *      bd:serviceParam wikibase:endpoint "en.wikipedia.org" .
 *      # Input params
 *      bd:serviceParam mwapi:titles "Albert Einstein" .
 *      # Output params
 *      ?title wikibase:output mwapi:title .
 *      ?item wikibase:outputItem mwapi:item .
 *      ?ns wikibase:output "@ns" .
 *
 *  }
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class MWApiServiceFactory extends AbstractServiceFactory {
    private static final Logger log = LoggerFactory.getLogger(MWApiServiceFactory.class);

    /**
     * Options configuring this service as a native Blazegraph service.
     */
    public static final BigdataNativeServiceOptions SERVICE_OPTIONS = new BigdataNativeServiceOptions();
    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(Ontology.NAMESPACE + "mwapi");
    /**
     * API type parameter name.
     */
    public static final URI API_KEY = new URIImpl(Ontology.NAMESPACE + "api");
    /**
     * Endpoint hostname parameter name.
     */
    public static final URI ENDPOINT_KEY = new URIImpl(Ontology.NAMESPACE + "endpoint");
    /**
     * Namespace for MWAPI parameters.
     */
    public static final String MWAPI_NAMESPACE = Mediawiki.NAMESPACE + "API/";
    /**
     * Default service config filename.
     */
    public static final String CONFIG_DEFAULT = "mwservices.json";
    /**
     * Config file parameter.
     */
    public static final String CONFIG_NAME = MWApiServiceFactory.class.getName() + ".config";
    /**
     * Filename of the config.
     */
    public static final String CONFIG_FILE = System.getProperty(CONFIG_NAME, CONFIG_DEFAULT);
    /**
     * Service config.
     */
    private final ServiceConfig config;

    public MWApiServiceFactory() throws IOException {
        log.info("Loading MWAPI service configuration from " + CONFIG_FILE);
        this.config = new ServiceConfig(new InputStreamReader(new FileInputStream(CONFIG_FILE), StandardCharsets.UTF_8));
        log.info("Registered " + config.size() + " services.");
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return SERVICE_OPTIONS;
    }

    /**
     * Register the service so it is recognized by Blazegraph.
     */
    public static void register() {
        ServiceRegistry reg = ServiceRegistry.getInstance();
        try {
            reg.add(SERVICE_KEY, new MWApiServiceFactory());
        } catch (IOException e) {
            // Do not add to whitelist if init failed.
            log.warn("MW Service registration failed: " + e);
            return;
        }
        reg.addWhitelistURL(SERVICE_KEY.toString());
    }

    @Override
    public BigdataServiceCall create(ServiceCallCreateParams params, final ServiceParams serviceParams) {
        ServiceNode serviceNode = params.getServiceNode();

        Preconditions.checkNotNull(serviceNode, "Missing service node?");

        try {
            ApiTemplate template = getServiceTemplate(serviceParams);

            return new MWApiServiceCall(template,
                getServiceHost(serviceParams),
                template.getInputVars(serviceParams),
                template.getOutputVars(serviceNode),
                params.getClientConnectionManager(),
                params.getTripleStore().getLexiconRelation()
                );
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Bad endpoint URL", e);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Cannot instantiate XML parser", e);
        }

    }

    /**
     * Extract service template name from params.
     * @param serviceParams
     * @return Service template
     */
    private ApiTemplate getServiceTemplate(final ServiceParams serviceParams) {
        final String templateName = serviceParams.getAsString(API_KEY);
        Preconditions.checkNotNull(templateName, "Service name (wikibase:api) should be supplied");
        serviceParams.clear(API_KEY);
        return config.getService(templateName);
    }

    /**
     * Get service host and check if it's valid.
     * @param serviceParams
     * @return Service endpoint hostname.
     * @throws MalformedURLException on bad URL
     */
    private String getServiceHost(final ServiceParams serviceParams) throws MalformedURLException {
        TermNode hostNode = serviceParams.get(ENDPOINT_KEY, null);
        Preconditions.checkNotNull(hostNode, "Service name (wikibase:endpoint) should be supplied");
        // TODO: allow variable endpoints
        Preconditions.checkArgument(hostNode.isConstant(), "Endpoint name should be a constant");

        serviceParams.clear(ENDPOINT_KEY);
        Value v = hostNode.getValue();
        final String endpointHost;
        if (v instanceof URI) {
            endpointHost = new URL(v.stringValue()).getHost();
        } else {
            endpointHost = v.stringValue();
        }
        if (!config.validEndpoint(endpointHost)) {
            throw new IllegalArgumentException("Host " + endpointHost + " is not allowed");
        }
        return endpointHost;
    }

    @Override
    public Set<IVariable<?>> getRequiredBound(final ServiceNode serviceNode) {
        ServiceParams params = serviceParamsFromNode(serviceNode);
        ApiTemplate api = getServiceTemplate(params);
        Map<String, IVariableOrConstant> potentialVars = api.getInputVars(params);

        // Extract params that have variables linked to them
        return potentialVars.entrySet().stream()
                .filter(entry -> entry.getValue() != null
                        && entry.getValue().isVar())
                .map(entry -> (IVariable<?>) entry.getValue())
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Get service params from Service Node.
     * FIXME: copypaste from ServiceParams.java, should be integrated there
     * @param serviceNode
     * @return
     */
    public ServiceParams serviceParamsFromNode(final ServiceNode serviceNode) {
        Preconditions.checkNotNull(serviceNode, "Service node is null?");

        final GraphPatternGroup<IGroupMemberNode> group = serviceNode.getGraphPattern();
        Preconditions.checkNotNull(serviceNode, "Group node is null?");

        final ServiceParams serviceParams = new ServiceParams();
        final Iterator<IGroupMemberNode> it = group.iterator();

        while (it.hasNext()) {
            final IGroupMemberNode node = it.next();

            if (node instanceof StatementPatternNode) {
                final StatementPatternNode sp = (StatementPatternNode) node;
                final TermNode s = sp.s();

                if (s.isConstant() && BD.SERVICE_PARAM.equals(s.getValue())) {
                    if (sp.p().isVariable()) {
                        throw new RuntimeException("not a valid service param triple pattern, "
                                        + "predicate must be constant: " + sp);
                    }

                    final URI param = (URI) sp.p().getValue();
                    serviceParams.add(param, sp.o());
                }
            }
        }

        return serviceParams;
    }

    /**
     * Create predicate parameter URI from name.
     * @param name
     * @return
     */
    public static URI paramNameToURI(String name) {
        return new URIImpl(MWAPI_NAMESPACE + name);
    }
}
