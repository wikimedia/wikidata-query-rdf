package org.wikidata.query.rdf.blazegraph.mwapi;

import java.net.MalformedURLException;
import java.net.URL;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;

/**
 * Representation of static or variable endpoint.
 * The resulting URL will always be https://HOSTNAME/w/api.php where hostname is either
 * taken from URI or from string.
 * It will be also checked against allowed endpoint whitelist.
 */
public abstract class Endpoint {

    /**
     * Get full endpoint URL relative to a binding set.
     *
     * @return Endpoint URL.
     * @throws MalformedURLException If it's bad URL
     */
    public abstract String getEndpointURL(IBindingSet binding) throws MalformedURLException;

    public static Endpoint create(IVariableOrConstant term, ServiceConfig config) throws MalformedURLException {
        if (term.isConstant()) {
            return new ConstantEndpoint(getURLFromValue(config, ((IV) term.get()).getValue()));
        } else {
            // Since term is not Constant, it must be a Var
            assert term instanceof Var;

            return new VariableEndpoint(config, (Var)term);
        }
    }

    private static String getURLFromValue(ServiceConfig config, Value v) throws MalformedURLException {
        String endpointHost = hostFromValue(v);
        if (!config.validEndpoint(endpointHost)) {
            throw new IllegalArgumentException("Host " + endpointHost + " is not allowed");
        }
        return new URL("https", endpointHost, "/w/api.php").toExternalForm();
    }

    private static String hostFromValue(Value v) throws MalformedURLException {
        if (v instanceof URI) {
            return new URL(v.stringValue()).getHost();
        } else {
            return v.stringValue();
        }
    }

    private static class ConstantEndpoint extends Endpoint {
        private final String endpoint;

        ConstantEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public String getEndpointURL(IBindingSet binding) {
            return endpoint;
        }
    }

    private static class VariableEndpoint extends Endpoint {
        private final ServiceConfig config;
        private final Var endpoint;

        VariableEndpoint(ServiceConfig config, Var endpoint) {
            this.config = config;
            this.endpoint = endpoint;
        }

        @Override
        public String getEndpointURL(IBindingSet binding) throws MalformedURLException {
            IV boundValue = (IV)endpoint.get(binding);
            if (boundValue == null) return null;

            return Endpoint.getURLFromValue(config, boundValue.getValue());
        }
    }
}
