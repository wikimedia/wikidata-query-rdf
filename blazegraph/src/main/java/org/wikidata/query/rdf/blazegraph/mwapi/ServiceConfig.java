package org.wikidata.query.rdf.blazegraph.mwapi;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

/**
 * MW API service configuration.
 */
public final class ServiceConfig {

    /**
     * Map of services, keyed by name.
     */
    private final Map<String, ApiTemplate> serviceMap;
    /**
     * Allowed endpoints.
     */
    private final List<String> endpoints;

    public ServiceConfig(Reader configReader) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mainNode = mapper.readTree(configReader);
        this.serviceMap = loadJSONConfig(mainNode.get("services"));
        this.endpoints = loadEndpoints(mainNode.get("endpoints"));
    }

    /**
     * Load set of configs from JSON config file.
     * @param node Services node
     * @return Map of API templates per name.
     */
    private static Map<String, ApiTemplate> loadJSONConfig(JsonNode node) {
        Preconditions.checkNotNull(node, "Must have services node");

        return Streams.stream(node.fieldNames())
                .collect(ImmutableMap.toImmutableMap(
                    fieldName -> fieldName,
                    fieldName -> ApiTemplate.fromJSON(node.get(fieldName))));
    }

    /**
     * Load list of endpoints.
     * @param node
     * @return
     */
    private static List<String> loadEndpoints(JsonNode node) {
        Preconditions.checkNotNull(node, "Must have endpoints node");
        Preconditions.checkArgument(node.isArray(), "Endpoints config should be an array");

        // Get immutable list of elements' text representations
        return Streams.stream(node.elements())
                .map(endpoint -> endpoint.asText())
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Get service template by name.
     * @param templateName
     * @return
     */
    public ApiTemplate getService(String templateName) {
        Preconditions.checkArgument(serviceMap.containsKey(templateName),
                "Service name " + templateName + " not found in configuration");
        return serviceMap.get(templateName);
    }

    /**
     * Check if endpoint is allowed.
     * @param endpointHost
     * @return
     */
    public boolean validEndpoint(String endpointHost) {
        for (String allowedEndpoint: endpoints) {
            if (endpointHost.endsWith(allowedEndpoint)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get number of services.
     * @return
     */
    public int size() {
        return serviceMap.size();
    }

}
