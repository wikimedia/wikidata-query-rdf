package org.wikidata.query.rdf.blazegraph;

import javax.servlet.ServletContextEvent;

import com.bigdata.rdf.sail.webapp.BigdataRDFServletContextListener;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Context listener to enact configurations we need on initialization.
 */
public class WikibaseContextListener extends BigdataRDFServletContextListener {

    @Override
    public void contextInitialized(final ServletContextEvent e) {
        super.contextInitialized(e);
        ServiceRegistry.getInstance().setDefaultServiceFactory(new DisableRemotesServiceFactory());
    }

    /**
     * Service factory that disables remote access.
     */
    private final class DisableRemotesServiceFactory implements ServiceFactory {

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
