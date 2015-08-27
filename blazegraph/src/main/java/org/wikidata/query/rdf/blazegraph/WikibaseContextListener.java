package org.wikidata.query.rdf.blazegraph;

import java.util.Map;

import javax.servlet.ServletContextEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseDateBOp;
import org.wikidata.query.rdf.blazegraph.constraints.WikibaseNowBOp;
import org.wikidata.query.rdf.blazegraph.label.LabelService;

import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.DateBOp.DateOp;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.FunctionRegistry.Factory;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactoryBase;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Context listener to enact configurations we need on initialization.
 */
public class WikibaseContextListener extends BigdataRDFServletContextListener {

    private static final Logger log = LoggerFactory.getLogger(WikibaseContextListener.class);

    /**
     * Replaces the default Blazegraph services with ones that do not allow
     * remote services and a label resolution service.
     */
    public static void initializeServices() {
        ServiceRegistry.getInstance().setDefaultServiceFactory(new DisableRemotesServiceFactory());
        LabelService.register();

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


        log.warn("Wikibase services initialized.");
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
     * Service factory that disables remote access.
     */
    private static final class DisableRemotesServiceFactory extends AbstractServiceFactoryBase {

        @Override
        public IServiceOptions getServiceOptions() {
            return null;
        }

        @Override
        public ServiceCall<?> create(ServiceCallCreateParams params) {
            throw new IllegalArgumentException("Service call not allowed: " + params.getServiceURI());
        }

    }

}
