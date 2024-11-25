package org.wikidata.query.rdf.blazegraph;

import static com.bigdata.rdf.sparql.ast.FunctionRegistry.checkArgs;
import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.collect.Lists.reverse;
import static org.wikidata.query.rdf.common.LoggingNames.MW_API_REQUEST;
import static org.wikidata.query.rdf.common.LoggingNames.REMOTE_REQUEST;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.servlet.ServletContextEvent;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.categories.CategoriesStoredQuery;
import org.wikidata.query.rdf.blazegraph.constraints.CoordinatePartBOp;
import org.wikidata.query.rdf.blazegraph.constraints.DecodeUriBOp;
import org.wikidata.query.rdf.blazegraph.constraints.IsSomeValueFunctionFactory;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseCornerBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseDateBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseDistanceBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseNowBOp;
import org.wikidata.query.rdf.blazegraph.geo.GeoService;
import org.wikidata.query.rdf.blazegraph.label.LabelService;
import org.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceCall;
import org.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceFactory;
import org.wikidata.query.rdf.common.uri.Dct;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Mediawiki;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.graph.impl.bd.GASService;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.DateBOp.DateOp;
import com.bigdata.rdf.sail.sparql.PrefixDeclProcessor;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.eval.SampleServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.SliceServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ValuesServiceFactory;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceFactoryImpl;
import com.bigdata.rdf.sparql.ast.service.RemoteServiceOptions;
import com.bigdata.rdf.sparql.ast.service.SPARQLVersion;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.store.BDS;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;

/**
 * Context listener to enact configurations we need on initialization.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "rawtypes"})
public class WikibaseContextListener extends BigdataRDFServletContextListener {

    private static final Logger LOG = LoggerFactory.getLogger(WikibaseContextListener.class);

    /**
     * Enable allowlist configuration.
     */
    private static final boolean ENABLE_ALLOWLIST = Boolean.parseBoolean(System.getProperty("wikibaseServiceEnableAllowlist", "true"));

    /**
     * Overrides the default namespace set by blazegraph on within the web.xml file.
     */
    private static final String DEFAULT_NAMESPACE = System.getProperty("blazegraphDefaultNamespace");

    /**
     * Default service allowlist filename.
     */
    private static final String ALLOWLIST_DEFAULT = "allowlist.txt";

    /**
     * Allowlist configuration name.
     */
    private static final String ALLOWLIST = System.getProperty("wikibaseServiceAllowlist", ALLOWLIST_DEFAULT);

    /**
     * Default metrics domain.
     */
    private static final String METRICS_DOMAIN_DEFAULT = "wdqs-blazegraph";

    /**
     * Default metrics domain configuration.
     */
    private static final String METRICS_DOMAIN = System.getProperty("WDQSMetricDomain", METRICS_DOMAIN_DEFAULT);

    public static final String BLAZEGRAPH_DEFAULT_NAMESPACE = "BLAZEGRAPH_DEFAULT_NAMESPACE";

    /**
     * Hooks that need to be run on shutdown.
     *
     * Stored in a thread safe list since the shutdown is not garanteed to run
     * in the same thread as the startup. And safe publication is just more cumbersome.
     */
    private final List<Runnable> shutdownHooks = new CopyOnWriteArrayList<>();

    /**
     * Initializes BG service setup to allow services from the allowlist.
     * Also add additional custom services and functions.
     */
    @VisibleForTesting
    public void initializeServices() {

        MetricRegistry metricRegistry = createMetricRegistry();

        // Enable service whitelisting
        final ServiceRegistry reg = ServiceRegistry.getInstance();
        reg.setWhitelistEnabled(ENABLE_ALLOWLIST);
        LabelService.register();
        GeoService.register();
        MWApiServiceFactory.register(metricRegistry.timer(name(MWApiServiceCall.class, MW_API_REQUEST)));
        CategoriesStoredQuery.register();

        // Allowlist services we like by default
        reg.addWhitelistURL(GASService.Options.SERVICE_KEY.toString());
        reg.addWhitelistURL(ValuesServiceFactory.SERVICE_KEY.toString());
        reg.addWhitelistURL(BDS.SEARCH_IN_SEARCH.toString());
        reg.addWhitelistURL(SliceServiceFactory.SERVICE_KEY.toString());
        reg.addWhitelistURL(SampleServiceFactory.SERVICE_KEY.toString());
        loadAllowlist(reg);

        // Initialize remote services
        reg.setDefaultServiceFactory(getDefaultServiceFactory(metricRegistry.timer(name(RemoteServiceFactoryImpl.class, REMOTE_REQUEST))));

        // Override date functions so that we can handle them
        // via WikibaseDate
        FunctionRegistry.remove(FunctionRegistry.YEAR);
        FunctionRegistry.add(FunctionRegistry.YEAR, getWikibaseDateBOpFactory(DateOp.YEAR));

        FunctionRegistry.remove(FunctionRegistry.MONTH);
        FunctionRegistry.add(FunctionRegistry.MONTH, getWikibaseDateBOpFactory(DateOp.MONTH));

        FunctionRegistry.remove(FunctionRegistry.DAY);
        FunctionRegistry.add(FunctionRegistry.DAY, getWikibaseDateBOpFactory(DateOp.DAY));

        FunctionRegistry.remove(FunctionRegistry.HOURS);
        FunctionRegistry.add(FunctionRegistry.HOURS, getWikibaseDateBOpFactory(DateOp.HOURS));

        FunctionRegistry.remove(FunctionRegistry.MINUTES);
        FunctionRegistry.add(FunctionRegistry.MINUTES, getWikibaseDateBOpFactory(DateOp.MINUTES));

        FunctionRegistry.remove(FunctionRegistry.SECONDS);
        FunctionRegistry.add(FunctionRegistry.SECONDS, getWikibaseDateBOpFactory(DateOp.SECONDS));

        FunctionRegistry.remove(FunctionRegistry.NOW);
        FunctionRegistry.add(FunctionRegistry.NOW, (context, globals, scalarValues, args) -> {

            if (args != null && args.length > 0)
                throw new IllegalArgumentException("no args for NOW()");

            return new WikibaseNowBOp(globals);
        });

        // Geospatial distance function
        FunctionRegistry.add(new URIImpl(GeoSparql.FUNCTION_NAMESPACE + "distance"), getDistanceBOPFactory());
        // Geospatial functions
        FunctionRegistry.add(new URIImpl(GeoSparql.NORTH_EAST_FUNCTION), getCornersBOPFactory(WikibaseCornerBOp.Corners.NE));
        FunctionRegistry.add(new URIImpl(GeoSparql.SOUTH_WEST_FUNCTION), getCornersBOPFactory(WikibaseCornerBOp.Corners.SW));
        FunctionRegistry.add(new URIImpl(GeoSparql.GLOBE_FUNCTION), getCoordinatePartBOpFactory(CoordinatePartBOp.Parts.GLOBE));
        FunctionRegistry.add(new URIImpl(GeoSparql.LON_FUNCTION), getCoordinatePartBOpFactory(CoordinatePartBOp.Parts.LON));
        FunctionRegistry.add(new URIImpl(GeoSparql.LAT_FUNCTION), getCoordinatePartBOpFactory(CoordinatePartBOp.Parts.LAT));
        // wikibase:decodeUri
        FunctionRegistry.add(new URIImpl(Ontology.NAMESPACE + "decodeUri"), getDecodeUriBOpFactory());
        IsSomeValueFunctionFactory.SomeValueMode mode = IsSomeValueFunctionFactory.SomeValueMode.lookup(System.getProperty("wikibaseSomeValueMode", "blank"));
        UrisScheme uris = UrisSchemeFactory.getURISystem();
        registerIsSomeValueFunction(FunctionRegistry::add, mode, uris.wellKnownBNodeIRIPrefix());
        addPrefixes(uris);

        LOG.info("Wikibase services initialized.");
    }

    /**
     * Get default service factory, with proper options.
     * @return Service factory
     */
    private static ServiceFactory getDefaultServiceFactory(Timer requestTimer) {
        final RemoteServiceOptions options = new RemoteServiceOptions();
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);
        options.setGET(true);
        options.setAcceptHeader(TupleQueryResultFormat.SPARQL.getDefaultMIMEType());
        return new MeteringRemoteServiceFactory(requestTimer, options);
    }

    /**
     * Load allowlist from file.
     */
    private static void loadAllowlist(final ServiceRegistry reg) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(ALLOWLIST),
                    StandardCharsets.UTF_8);
            for (String line : lines) {
                registerAllowedFederatedService(line, reg::addWhitelistURL, reg::addAlias);
            }
        } catch (FileNotFoundException e) {
            // ignore file not found
            LOG.info("Allowlist file {} not found, ignoring.", ALLOWLIST);
        } catch (IOException e) {
            LOG.warn("Failed reading from allowlist file");
        }
    }

    @VisibleForTesting
    static void registerAllowedFederatedService(String line, Consumer<String> allowlist, BiConsumer<URI, URI> aliases) {
        List<String> serviceAndAliases = Arrays.stream(line.split(","))
                .map(String::trim)
                .filter(r -> !r.isEmpty())
                .collect(Collectors.toList());

        IntStream.range(0, serviceAndAliases.size()).forEach(i -> {
            String service = serviceAndAliases.get(i);
            if (i == 0) {
                allowlist.accept(service);
            } else {
                allowlist.accept(service);
                aliases.accept(ValueFactoryImpl.getInstance().createURI(serviceAndAliases.get(0)),
                        ValueFactoryImpl.getInstance().createURI(service));
            }
        });

    }

    /**
     * Add a prefix to the system only if the prefix has not been declared previously.
     * @param decls prefix declarations so far
     * @param prefix new prefix to check and add
     * @param uri uri for the prefix
     */
    private static void addDeclIfNew(Map<String, String> decls, String prefix, String uri) {
        if (!decls.containsKey(prefix)) {
            decls.put(prefix, uri);
        }
    }

    /**
     * Add standard prefixes to the system, but only if they have not already been declared, e.g. in a prefixes.conf
     * @param uris Wikidata URIs to use
     */
    private static void addPrefixes(final UrisScheme uris) {
        final Map<String, String> defaultDecls = PrefixDeclProcessor.defaultDecls;
        for (PropertyType p: PropertyType.values()) {
            addDeclIfNew(defaultDecls, p.prefix(), uris.property(p));
        }
        addDeclIfNew(defaultDecls, "wikibase", Ontology.NAMESPACE);
        uris.entityPrefixes().forEach((key, value) -> addDeclIfNew(defaultDecls, key, value));
        addDeclIfNew(defaultDecls, "wds", uris.statement());
        addDeclIfNew(defaultDecls, "wdv", uris.value());
        addDeclIfNew(defaultDecls, "wdref", uris.reference());
        // External schemata
        addDeclIfNew(defaultDecls, "schema", SchemaDotOrg.NAMESPACE);
        addDeclIfNew(defaultDecls, "prov", Provenance.NAMESPACE);
        addDeclIfNew(defaultDecls, "skos", SKOS.NAMESPACE);
        addDeclIfNew(defaultDecls, "owl", OWL.NAMESPACE);
        addDeclIfNew(defaultDecls, "geo", GeoSparql.NAMESPACE);
        addDeclIfNew(defaultDecls, "geof", GeoSparql.FUNCTION_NAMESPACE);
        addDeclIfNew(defaultDecls, "mediawiki", Mediawiki.NAMESPACE);
        addDeclIfNew(defaultDecls, "mwapi", Mediawiki.API);
        addDeclIfNew(defaultDecls, "gas", GASService.Options.NAMESPACE);
        addDeclIfNew(defaultDecls, "ontolex", Ontolex.NAMESPACE);
        addDeclIfNew(defaultDecls, "dct", Dct.NAMESPACE);
        addDeclIfNew(defaultDecls, "wikibaseqs", Ontology.QueryService.QUERY_SERVICE_NS);
    }

    private MetricRegistry createMetricRegistry() {
        MetricRegistry registry = new MetricRegistry();
        JmxReporter jmxReporter = JmxReporter.forRegistry(registry).inDomain(METRICS_DOMAIN).build();
        jmxReporter.start();
        shutdownHooks.add(jmxReporter::stop);
        return registry;
    }


    @Override
    public void contextInitialized(final ServletContextEvent sce) {
        if (DEFAULT_NAMESPACE != null) {
            Map<String, String> overrides;
            if (sce.getServletContext().getAttribute("INIT_PARAMS_OVERRIDES") != null) {
                overrides = (Map<String, String>) sce.getServletContext().getAttribute("INIT_PARAMS_OVERRIDES");
            } else {
                overrides = new HashMap<>();
                sce.getServletContext().setAttribute("INIT_PARAMS_OVERRIDES", overrides);
            }
            overrides.put("namespace", DEFAULT_NAMESPACE);
        }
        super.contextInitialized(sce);
        sce.getServletContext().setAttribute(BLAZEGRAPH_DEFAULT_NAMESPACE, this.getBigdataRDFContext().getConfig().namespace);
        initializeServices();
    }

    @Override
    public void contextDestroyed(ServletContextEvent e) {
        shutdown();
        super.contextDestroyed(e);
    }

    @VisibleForTesting
    public void shutdown() {
        // execute shutdownHooks in reverse order to ensure dependencies are respected.
        for (Runnable hook : reverse(shutdownHooks)) {
            hook.run();
        }
    }

    /**
     * Create factory for specific WikibaseDateOp operation.
     * @param dateop Parent DateOp object
     * @return Factory object to create WikibaseDateBOp
     */
    private static Factory getWikibaseDateBOpFactory(final DateOp dateop) {
        return (context, globals, scalarValues, args) -> {

            checkArgs(args, ValueExpressionNode.class);

            final IValueExpression<? extends IV> left =
                AST2BOpUtility.toVE(context, globals, args[0]);

            return new WikibaseDateBOp(left, dateop, globals);
        };
    }

    /**
     * Get WikibaseDistanceBOp factory.
     * @return Factory to create WikibaseDistanceBOp
     */
    private static Factory getDistanceBOPFactory() {
        return (context, globals, scalarValues, args) -> {

            checkArgs(args, ValueExpressionNode.class, ValueExpressionNode.class);

            final IValueExpression<? extends IV> left = AST2BOpUtility.toVE(context,
                    globals, args[0]);

            final IValueExpression<? extends IV> right = AST2BOpUtility
                        .toVE(context, globals, args[1]);

            return new WikibaseDistanceBOp(left, right, globals);
        };
    }

    /**
     * Get WikibaseDistanceBOp factory.
     * @return Factory to create WikibaseDistanceBOp
     */
    private static Factory getCornersBOPFactory(final WikibaseCornerBOp.Corners corner) {
        return (context, globals, scalarValues, args) -> {

            checkArgs(args, ValueExpressionNode.class, ValueExpressionNode.class);

            final IValueExpression<? extends IV> left = AST2BOpUtility.toVE(context,
                    globals, args[0]);

            final IValueExpression<? extends IV> right = AST2BOpUtility
                        .toVE(context, globals, args[1]);

            return new WikibaseCornerBOp(left, right, corner, globals);
        };
    }

    /**
     * Get DecodeUriBOp factory.
     * @return Factory to create DecodeUriBOp
     */
    @SuppressWarnings("unchecked")
    private static Factory getDecodeUriBOpFactory() {
        return (context, globals, scalarValues, args) -> {

            checkArgs(args, ValueExpressionNode.class);

            final IValueExpression ve = AST2BOpUtility.toVE(context, globals, args[0]);

            return new DecodeUriBOp(ve, globals);
        };
    }

    /**
     * Get CoordinatePartBOp factory.
     * @return Factory to create CoordinatePartBOp
     */
    @SuppressWarnings("unchecked")
    private static Factory getCoordinatePartBOpFactory(final CoordinatePartBOp.Parts part) {
        return (context, globals, scalarValues, args) -> {

            checkArgs(args, ValueExpressionNode.class);

            final IValueExpression ve = AST2BOpUtility.toVE(context, globals, args[0]);

            return new CoordinatePartBOp(ve, part, globals);
        };
    }

    /**
     * Service factory that disables remote access.
     */
    private static final class DisableRemotesServiceFactory extends AbstractServiceFactoryBase {

        /**
         * Default service options.
         * We use remote since that's how we get to this service in the first place -
         * local ones should have been served already.
         */
        private static final IServiceOptions OPTIONS = new RemoteServiceOptions();

        @Override
        public IServiceOptions getServiceOptions() {
            return OPTIONS;
        }

        @Override
        public ServiceCall<?> create(ServiceCallCreateParams params) {
            throw new IllegalArgumentException("Service call not allowed: " + params.getServiceURI());
        }

    }

    public static void registerIsSomeValueFunction(BiConsumer<URI, FunctionRegistry.Factory> registry,
                                                   IsSomeValueFunctionFactory.SomeValueMode mode,
                                                   String skolemUri
    ) {
        // wikibase:isSomeValue, meant to filter https://www.mediawiki.org/wiki/Wikibase/DataModel#PropertySomeValueSnak
        registry.accept(IsSomeValueFunctionFactory.IS_SOMEVALUE_FUNCTION_URI, new IsSomeValueFunctionFactory(mode, skolemUri));
    }
}
