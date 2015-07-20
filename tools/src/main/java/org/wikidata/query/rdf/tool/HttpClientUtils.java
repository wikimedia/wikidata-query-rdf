package org.wikidata.query.rdf.tool;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;

/**
 * Utilities for dealing with HttpClient.
 */
public final class HttpClientUtils {
    /**
     * Configure request to ignore cookies.
     */
    public static void ignoreCookies(HttpRequestBase request) {
        RequestConfig noCookiesConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build();
        request.setConfig(noCookiesConfig);
    }

    private HttpClientUtils() {
        // Uncallable utility constructor
    }
}
