package org.wikidata.query.rdf.blazegraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContextEvent;

import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseCornerBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseDateBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseDistanceBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseNowBOp;
import org.wikidata.query.rdf.blazegraph.geo.GeoService;
import org.wikidata.query.rdf.blazegraph.label.LabelService;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.graph.impl.bd.GASService;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.DateBOp.DateOp;
import com.bigdata.rdf.sail.sparql.PrefixDeclProcessor;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;
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

/**
 * Context listener to enact configurations we need on initialization.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class WikibaseContextListener extends BigdataRDFServletContextListener {

    private static final Logger log = LoggerFactory.getLogger(WikibaseContextListener.class);

    /**
     * Default service whitelist filename.
     */
    public static final String WHITELIST_DEFAULT = "whitelist.txt";

    /**
     * Whitelist configuration name.
     */
    public static final String WHITELIST = System.getProperty("wikibaseServiceWhitelist", WHITELIST_DEFAULT);

    /**
     * Initializes BG service setup to allow whitelisted services.
     * Also add additional custom services and functions.
     */
    public static void initializeServices() {
        // Enable service whitelisting
        final ServiceRegistry reg = ServiceRegistry.getInstance();
        reg.setWhitelistEnabled(true);
        LabelService.register();
        GeoService.register();

        // Whitelist services we like by default
        reg.addWhitelistURL(GASService.Options.SERVICE_KEY.toString());
        reg.addWhitelistURL(ValuesServiceFactory.SERVICE_KEY.toString());
        reg.addWhitelistURL(BDS.SEARCH_IN_SEARCH.toString());
        reg.addWhitelistURL(SliceServiceFactory.SERVICE_KEY.toString());
        reg.addWhitelistURL(SampleServiceFactory.SERVICE_KEY.toString());
        loadWhitelist(reg);

        // Initialize remote services
        reg.setDefaultServiceFactory(getDefaultServiceFactory());

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
        FunctionRegistry.add(FunctionRegistry.NOW, new Factory() {
            public IValueExpression<? extends IV> create(final BOpContextBase context, final GlobalAnnotations globals,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                if (args != null && args.length > 0)
                    throw new IllegalArgumentException("no args for NOW()");

                return new WikibaseNowBOp(globals);
            }
        });

        // Geospatial distance function
        FunctionRegistry.add(new URIImpl(GeoSparql.FUNCTION_NAMESPACE + "distance"), getDistanceBOPFactory());
        FunctionRegistry.add(new URIImpl(GeoSparql.NORTH_EAST_FUNCTION), getCornersBOPFactory(WikibaseCornerBOp.Corners.NE));
        FunctionRegistry.add(new URIImpl(GeoSparql.SOUTH_WEST_FUNCTION), getCornersBOPFactory(WikibaseCornerBOp.Corners.SW));

        addPrefixes(WikibaseUris.getURISystem());

        log.warn("Wikibase services initialized.");
    }

    /**
     * Get default service factory, with proper options.
     * @return
     */
    private static ServiceFactory getDefaultServiceFactory() {
        final RemoteServiceOptions options = new RemoteServiceOptions();
        options.setSPARQLVersion(SPARQLVersion.SPARQL_11);
        options.setGET(true);
        return new RemoteServiceFactoryImpl(options);
    }

    /**
     * Load whitelist from file.
     * @param reg
     */
    private static void loadWhitelist(final ServiceRegistry reg) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(WHITELIST),
                    StandardCharsets.UTF_8);
            for (String line : lines) {
                reg.addWhitelistURL(line);
            }
        } catch (FileNotFoundException e) {
            // ignore file not found
            log.info("Whitelist file {} not found, ignoring.", WHITELIST);
        } catch (IOException e) {
            log.warn("Failed reading from whitelist file");
        }
    }

    /**
     * Add standard prefixes to the system.
     * @param uris Wikidata URIs to use
     */
    private static void addPrefixes(final WikibaseUris uris) {
        final Map<String, String> defaultDecls = PrefixDeclProcessor.defaultDecls;
        for (PropertyType p: PropertyType.values()) {
            defaultDecls.put(p.prefix(), uris.property(p));
        }
        defaultDecls.put("wikibase", Ontology.NAMESPACE);
        defaultDecls.put("wd", uris.entity());
        defaultDecls.put("wds", uris.statement());
        defaultDecls.put("wdv", uris.value());
        defaultDecls.put("wdref", uris.reference());
        defaultDecls.put("wdata", uris.entityData());
        // External schemata
        defaultDecls.put("schema", SchemaDotOrg.NAMESPACE);
        defaultDecls.put("prov", Provenance.NAMESPACE);
        defaultDecls.put("skos", SKOS.NAMESPACE);
        defaultDecls.put("owl", OWL.NAMESPACE);
        defaultDecls.put("geo", GeoSparql.NAMESPACE);
        defaultDecls.put("geof", GeoSparql.FUNCTION_NAMESPACE);
        // Add supplemental prefixes
        final Map<String, String> extra = uris.getExtraPrefixes();
        for (String pref: extra.keySet()) {
            defaultDecls.put(pref, extra.get(pref));
        }
    }

    @Override
    public void contextInitialized(final ServletContextEvent e) {
        super.contextInitialized(e);
        initializeServices();
    }

    /**
     * Create factory for specific WikibaseDateOp operation.
     * @param dateop
     * @return Factory object to create WikibaseDateBOp
     */
    private static Factory getWikibaseDateBOpFactory(final DateOp dateop) {
        return new Factory() {
            public IValueExpression<? extends IV> create(final BOpContextBase context,
                    final GlobalAnnotations globals, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                FunctionRegistry.checkArgs(args,
                        ValueExpressionNode.class);

                final IValueExpression<? extends IV> left =
                    AST2BOpUtility.toVE(context, globals, args[0]);

                return new WikibaseDateBOp(left, dateop, globals);
            }
        };
    }

    /**
     * Get WikibaseDistanceBOp factory.
     * @return Factory to create WikibaseDistanceBOp
     */
    private static Factory getDistanceBOPFactory() {
        return new Factory() {
            public IValueExpression<? extends IV> create(final BOpContextBase context, final GlobalAnnotations globals,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                if (args.length < 2) {
                    throw new IllegalArgumentException("wrong # of args");
                }

                final IValueExpression<? extends IV> left = AST2BOpUtility.toVE(context,
                        globals, args[0]);

                final IValueExpression<? extends IV> right = AST2BOpUtility
                            .toVE(context, globals, args[1]);

                return new WikibaseDistanceBOp(left, right, globals);
            }
        };
    }

    /**
     * Get WikibaseDistanceBOp factory.
     * @return Factory to create WikibaseDistanceBOp
     */
    private static Factory getCornersBOPFactory(final WikibaseCornerBOp.Corners corner) {
        return new Factory() {
            public IValueExpression<? extends IV> create(final BOpContextBase context, final GlobalAnnotations globals,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                if (args.length < 2) {
                    throw new IllegalArgumentException("wrong # of args");
                }

                final IValueExpression<? extends IV> left = AST2BOpUtility.toVE(context,
                        globals, args[0]);

                final IValueExpression<? extends IV> right = AST2BOpUtility
                            .toVE(context, globals, args[1]);

                return new WikibaseCornerBOp(left, right, corner, globals);
            }
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

}
