package org.wikidata.query.rdf.blazegraph;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;

import com.bigdata.rdf.sail.webapp.client.DefaultHttpClientFactory;
import com.bigdata.rdf.sail.webapp.client.IHttpClientFactory;

/**
 * Http client factory that knows about proxy settings.
 */
public class ProxiedHttpConnectionFactory implements IHttpClientFactory {

    /**
     * Default factory instance.
     */
    private final DefaultHttpClientFactory defaultFactory;

    /**
     * Configuration name for proxy host.
     */
    private static final String HTTP_PROXY = "http.proxyHost";
    /**
     * Configuration name for proxy port.
     */
    private static final String HTTP_PROXY_PORT = "http.proxyPort";
    /**
     * Configuration name for User agent.
     */
    private static final String HTTP_USER_AGENT = "http.userAgent";
    public ProxiedHttpConnectionFactory() {
        defaultFactory = new DefaultHttpClientFactory();
    }

    @Override
    public HttpClient newInstance() {
        final HttpClient client = defaultFactory.newInstance();

        if (System.getProperty(HTTP_PROXY) != null
                && System.getProperty(HTTP_PROXY_PORT) != null) {
            final ProxyConfiguration proxyConfig = client.getProxyConfiguration();
            final HttpProxy proxy = new HttpProxy(
                    System.getProperty(HTTP_PROXY),
                    Integer.parseInt(System.getProperty(HTTP_PROXY_PORT)));
            proxy.getExcludedAddresses().add("localhost");
            proxy.getExcludedAddresses().add("127.0.0.1");
            proxyConfig.getProxies().add(proxy);
        }
        final String userAgent = System.getProperty(HTTP_USER_AGENT);
        if (userAgent != null) {
            client.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, userAgent));
        }

        return client;
    }

}
