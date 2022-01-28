package org.wikidata.query.rdf.mwoauth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletConfig;

import org.apache.http.HttpHost;

public final class OAuthProxyConfig {

    private final ServletConfig servletConfig;

    static final String SYSTEM_PROPERTY_PREFIX = OAuthProxyConfig.class.getName() + ".";
    static final String ACCESS_TOKEN_DURATION_HOURS = "accessTokenDurationHours";
    static final String ACCESS_TOKEN_SECRET = "accessTokenSecret";
    static final String CONSUMER_KEY_PROPERTY = "consumerKey";
    static final String CONSUMER_SECRET_PROPERTY = "consumerSecret";
    static final String INDEX_URL_PROPERTY = "indexUrl";
    static final String NICE_URL_BASE_PROPERTY = "niceUrlBase";
    static final String WIKI_LOGOUT_LINK_PROPERTY = "wikiLogoutLink";
    static final String SUCCESS_REDIRECT_PROPERTY = "successRedirect";
    static final String BANNED_USERNAMES_PATH_PROPERTY = "bannedUsernamesPath";

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

    public String indexUrl() {
        return loadStringParam(INDEX_URL_PROPERTY);
    }

    public String niceUrlBase() {
        return loadStringParam(NICE_URL_BASE_PROPERTY);
    }

    public String successRedirect() {
        return loadStringParam(SUCCESS_REDIRECT_PROPERTY, "/");
    }

    public String accessTokenSecret() {
        return loadStringParam(ACCESS_TOKEN_SECRET);
    }

    public Duration accessTokenDuration() {
        return Duration.ofHours(Integer.parseInt(loadStringParam(
            ACCESS_TOKEN_DURATION_HOURS, "2")));
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

    public Set<String> bannedUsernames() {
        String path = loadStringParam(BANNED_USERNAMES_PATH_PROPERTY);
        if (path == null || path.isEmpty()) {
            return Collections.emptySet();
        }
        try (BufferedReader br = Files.newBufferedReader(Paths.get(path))) {
             // One banned name per line.
             // Names must be in display form (with spaces, no underscores).
            return br.lines()
                .filter(l -> !l.isEmpty())
                .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed reading from " + path, e);
        }
    }

    private String loadStringParam(String property) {
        return loadStringParam(property, null);
    }

    private String loadStringParam(String property, String def) {
        String value = servletConfig != null ? servletConfig.getInitParameter(property) : null;
        return value != null ? value : System.getProperty(SYSTEM_PROPERTY_PREFIX + property, def);
    }
}
