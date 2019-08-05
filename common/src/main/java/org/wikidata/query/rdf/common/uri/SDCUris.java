package org.wikidata.query.rdf.common.uri;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * URI scheme for SDC.
 * Unlike standard Wikidata URI scheme, it has two bases - Wikibase and Commons.
 * The URIs are as follows:
 * root - Commons
 * entityData - Commons
 * entity - Commons
 * statement - Commons
 * properties - Wikidata
 * reference/value - Commons
 * TODO: sdc and M prefixes are hardcoded for now.
 */
public class SDCUris extends WikibaseUris {
    /**
     * Prefix for entity URI.
     */
    public static final String ENTITY_PREFIX = "sdc";
    /**
     * Prefix for entity data URI.
     */
    public static final String ENTITY_DATA_PREFIX = "sdcdata";
    /**
     * Initial letter for mediainfo items.
     */
    private static final String MEDIAINFO_INITIAL = "M";

    /**
     * Federated (Wikidata) URI scheme.
     */
    private final WikibaseUris federatedUris;

    public SDCUris(URI commonsUrl, URI wikidataUrl) {
        super(commonsUrl);
        federatedUris = new WikibaseUris(wikidataUrl);
    }

    @Override
    public String entityIdToURI(String entityId) {
        if (entityId.startsWith(MEDIAINFO_INITIAL)) {
            return super.entityIdToURI(entityId);
        }
        return federatedUris.entityIdToURI(entityId);
    }

    @Override
    public String entityURItoId(String uri) {
        if (uri.startsWith(root())) {
            return super.entityURItoId(uri);
        }
        return federatedUris.entityURItoId(uri);
    }

    @Override
    public boolean isEntityURI(String uri) {
        if (uri.startsWith(root())) {
            return super.isEntityURI(uri);
        }
        return federatedUris.isEntityURI(uri);
    }

    @Override
    public Collection<String> entityURIs() {
        return ImmutableList.<String>builder()
                .addAll(federatedUris.entityURIs())
                .addAll(super.entityURIs())
                .build();
    }

    @Override
    public Map<String, String> entityPrefixes() {
        return ImmutableMap.<String, String>builder()
                .putAll(federatedUris.entityPrefixes())
                .put(ENTITY_PREFIX, super.entity())
                .put(ENTITY_DATA_PREFIX, super.entityData())
                .build();
    }

    @Override
    public Collection<String> entityInitials() {
        return ImmutableList.<String>builder()
                .addAll(super.entityInitials())
                .add(MEDIAINFO_INITIAL)
                .build();
    }

    public String property(String suffix) {
        return federatedUris.property(suffix);
    }
}
