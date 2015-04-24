package org.wikidata.query.rdf.blazegraph;

import org.openrdf.model.URI;

import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceFactory;

/**
 * ServiceFactory that sends service calls that match a prefix to a different
 * factory than those that don't. Useful for setting ServiceRegistry's
 * defaultService so some prefixes can be reserved for different types of
 * services.
 */
public class PrefixDelegatingServiceFactory implements ServiceFactory {
    /**
     * Service factory to use if the prefix doesn't match.
     */
    private final ServiceFactory defaultFactory;
    /**
     * Prefix to check.
     */
    private final String prefix;
    /**
     * Service factory to use if the prefix does match.
     */
    private final ServiceFactory prefixedFactory;

    public PrefixDelegatingServiceFactory(ServiceFactory defaultFactory, String prefix, ServiceFactory prefixedFactory) {
        this.defaultFactory = defaultFactory;
        this.prefix = prefix;
        this.prefixedFactory = prefixedFactory;
    }

    @Override
    public IServiceOptions getServiceOptions() {
        /*
         * Sadly we can't figure out which service options to use so we just use
         * the default ones. This is almost certainly wrong bug doesn't seem to
         * cause any trouble yet.
         */
        return defaultFactory.getServiceOptions();
    }

    @Override
    public ServiceCall<?> create(ServiceCallCreateParams params) {
        return getServiceFactory(params.getServiceURI()).create(params);
    }

    /**
     * Get the service factory to use given this URI.
     */
    private ServiceFactory getServiceFactory(URI uri) {
        if (uri.stringValue().startsWith(prefix)) {
            return prefixedFactory;
        }
        return defaultFactory;
    }
}
