package org.wikidata.query.rdf.mwoauth;

import javax.servlet.ServletConfig;

public final class OAuthProxyConfig {

    private final ServletConfig servletConfig;

    static final String SYSTEM_PROPERTY_PREFIX = OAuthProxyConfig.class.getName() + ".";
    static final String CONSUMER_KEY_PROPERTY = "consumerKey";
    static final String CONSUMER_SECRET_PROPERTY = "consumerSecret";
    static final String SESSIONS_STORE_LIMIT_PROPERTY = "sessionStoreLimit";
    static final String INDEX_URL_PROPERTY = "indexUrl";
    static final String NICE_URL_BASE_PROPERTY = "niceUrlBase";
    static final String WIKI_LOGOUT_LINK_PROPERTY = "wikiLogoutLink";



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

    public String wikiLogoutLink() {
        return loadStringParam(WIKI_LOGOUT_LINK_PROPERTY);
    }
    private String loadStringParam(String property) {
        String value = servletConfig != null ? servletConfig.getInitParameter(property) : null;
        return value != null ? value : System.getProperty(SYSTEM_PROPERTY_PREFIX + property);
    }
}
