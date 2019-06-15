package org.wikidata.query.rdf.blazegraph.mwapi;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import com.codahale.metrics.Timer;
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
     * Parameter setting global limit across continuations.
     */
    public static final URI LIMIT_KEY = new URIImpl(Ontology.NAMESPACE + "limit");
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
    private final Timer requestTimer;

    public MWApiServiceFactory(Timer requestTimer) throws IOException {
        log.info("Loading MWAPI service configuration from {}", CONFIG_FILE);
        this.config = new ServiceConfig(Files.newBufferedReader(Paths.get(CONFIG_FILE), StandardCharsets.UTF_8));
        log.info("Registered {} services.", config.size());
        this.requestTimer = requestTimer;
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return SERVICE_OPTIONS;
    }

    /**
     * Register the service so it is recognized by Blazegraph.
     * @param requestTimer
     */
    public static void register(Timer requestTimer) {
        ServiceRegistry reg = ServiceRegistry.getInstance();
        try {
            reg.add(SERVICE_KEY, new MWApiServiceFactory(requestTimer));
        } catch (IOException e) {
            // Do not add to whitelist if init failed.
            log.warn("MW Service registration failed.", e);
            return;
        }
        reg.addWhitelistURL(SERVICE_KEY.toString());
    }

    @Override
    public BigdataServiceCall create(ServiceCallCreateParams params, final ServiceParams serviceParams) {
        ServiceNode serviceNode = params.getServiceNode();

        requireNonNull(serviceNode, "Missing service node?");

        try {
            ApiTemplate template = getServiceTemplate(serviceParams);

            return new MWApiServiceCall(template,
                    getServiceHost(serviceParams),
                    template.getInputVars(serviceParams),
                    template.getOutputVars(serviceNode),
                    params.getClientConnectionManager(),
                    params.getTripleStore().getLexiconRelation(),
                    requestTimer,
                    getLimit(serviceParams));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Bad endpoint URL", e);
        }
    }

    /**
     * Extract service template name from params.
     * @param serviceParams
     * @return Service template
     */
    private ApiTemplate getServiceTemplate(final ServiceParams serviceParams) {
        final String templateName = serviceParams.getAsString(API_KEY);
        requireNonNull(templateName, "Service name (wikibase:api) should be supplied");
        serviceParams.clear(API_KEY);
        return config.getService(templateName);
    }

    /**
     * Get service host and check if it's valid.
     * @return Service endpoint.
     * @throws MalformedURLException on bad URL
     */
    private Endpoint getServiceHost(final ServiceParams serviceParams) throws MalformedURLException {
        TermNode hostNode = serviceParams.get(ENDPOINT_KEY, null);
        requireNonNull(hostNode, "Service name (wikibase:endpoint) should be supplied");
        serviceParams.clear(ENDPOINT_KEY);
        return Endpoint.create(hostNode.getValueExpression(), config);
    }

    /**
     * Get limit configuration from service node params.
     * @return Limit (0 if none specified, -1 if it's no continuations)
     */
    private int getLimit(final ServiceParams serviceParams) {
        TermNode limitNode = serviceParams.get(LIMIT_KEY, null);
        if (limitNode == null) {
            return 0;
        }
        serviceParams.clear(LIMIT_KEY);
        Value v = limitNode.getValue();
        if (v.stringValue().equals("once")) {
            return -1;
        }
        return Integer.parseInt(v.stringValue());
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
     */
    public ServiceParams serviceParamsFromNode(final ServiceNode serviceNode) {
        requireNonNull(serviceNode, "Service node is null?");

        final GraphPatternGroup<IGroupMemberNode> group = serviceNode.getGraphPattern();
        requireNonNull(serviceNode, "Group node is null?");

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
     */
    public static URI paramNameToURI(String name) {
        return new URIImpl(MWAPI_NAMESPACE + name);
    }
}
