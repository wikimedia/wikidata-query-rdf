package org.wikidata.query.rdf.blazegraph.events;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"method", "client_ip", "request_headers", "has_cookies", "status_code"})
public class EventHttpMetadata {
    private final String method;
    private final String clientIp;
    private final Map<String, String> requestHeaders;
    private final boolean cookies;
    private final int statusCode;

    public EventHttpMetadata(String method, String clientIp, Map<String, String> requestHeaders, boolean cookies, int statusCode) {
        this.method = method;
        this.clientIp = clientIp;
        this.requestHeaders = requestHeaders;
        this.cookies = cookies;
        this.statusCode = statusCode;
    }

    public String getMethod() {
        return method;
    }

    @JsonProperty("client_ip")
    public String getClientIp() {
        return clientIp;
    }

    @JsonProperty("request_headers")
    public Map<String, String> getRequestHeaders() {
        return requestHeaders;
    }

    @JsonProperty("has_cookies")
    public boolean hasCookies() {
        return cookies;
    }

    @JsonProperty("status_code")
    public int getStatusCode() {
        return statusCode;
    }
}
