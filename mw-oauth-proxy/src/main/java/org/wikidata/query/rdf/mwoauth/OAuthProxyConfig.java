package org.wikidata.query.rdf.mwoauth;

import javax.servlet.ServletConfig;

import org.apache.http.HttpHost;

public final class OAuthProxyConfig {

    private final ServletConfig servletConfig;

    static final String SYSTEM_PROPERTY_PREFIX = OAuthProxyConfig.class.getName() + ".";
    static final String CONSUMER_KEY_PROPERTY = "consumerKey";
    static final String CONSUMER_SECRET_PROPERTY = "consumerSecret";
    static final String SESSIONS_STORE_LIMIT_PROPERTY = "sessionStoreLimit";
    static final String INDEX_URL_PROPERTY = "indexUrl";
    static final String NICE_URL_BASE_PROPERTY = "niceUrlBase";
    static final String WIKI_LOGOUT_LINK_PROPERTY = "wikiLogoutLink";

    static final String SESSION_STORE_HOSTNAME = "sessionStoreHostname";
    static final String SESSION_STORE_PORT = "sessionStorePort";
    static final String SESSION_STORE_SCHEME = "sessionStoreScheme";
    static final String SESSION_STORE_KEY_PREFIX = "sessionStoreKeyPrefix";

    public OAuthProxyConfig(ServletConfig servletConfig) {
        this.servletConfig = servletConfig;
    }

    public String consumerKey() {
        return loadStringParam(CONSUMER_KEY_PROPERTY);
    }

    public String consumerSecret() {
        return loadStringParam(CONSUMER_SECRET_PROPERTY);
    }

    public int sessionStoreLimit() {
        return Integer.parseInt(loadStringParam(SESSIONS_STORE_LIMIT_PROPERTY));
    }

    public String indexUrl() {
        return loadStringParam(INDEX_URL_PROPERTY);
    }

    public String niceUrlBase() {
        return loadStringParam(NICE_URL_BASE_PROPERTY);
    }

    public String sessionStoreScheme() {
        return loadStringParam(SESSION_STORE_SCHEME, "https");
    }

    public String sessionStoreHostname() {
        return loadStringParam(SESSION_STORE_HOSTNAME, "localhost");
    }

    public int sessionStorePort() {
        return Integer.parseInt(loadStringParam(SESSION_STORE_PORT, "8080"));
    }

    public HttpHost sessionStoreHost() {
        return new HttpHost(sessionStoreHostname(), sessionStorePort(), sessionStoreScheme());
    }

    public String sessionStoreKeyPrefix() {
        return loadStringParam(SESSION_STORE_KEY_PREFIX);
    }

    public String wikiLogoutLink() {
        return loadStringParam(WIKI_LOGOUT_LINK_PROPERTY);
    }


    private String loadStringParam(String property) {
        return loadStringParam(property, null);
    }

    private String loadStringParam(String property, String def) {
        String value = servletConfig != null ? servletConfig.getInitParameter(property) : null;
        return value != null ? value : System.getProperty(SYSTEM_PROPERTY_PREFIX + property, def);
    }
}
