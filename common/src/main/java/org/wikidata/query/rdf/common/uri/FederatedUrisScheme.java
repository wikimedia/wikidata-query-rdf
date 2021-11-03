package org.wikidata.query.rdf.common.uri;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

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
@Immutable
public class FederatedUrisScheme implements UrisScheme {

    /**
     * Main URIs.
     *
     * Operations are done on those URIs first if supported.
     */
    private final DefaultUrisScheme mainUris;

    /**
     * Federated (Wikidata) URI scheme.
     *
     * Fallback to those URIs when operation isn't supported on the main URIs
     */
    private final UrisScheme federatedUris;
    private final List<String> entityURIs;
    private final Map<String, String> entityPrefixes;
    private final List<String> entityInitials;

    public FederatedUrisScheme(DefaultUrisScheme mainUris, UrisScheme federatedUris) {
        this.mainUris = mainUris;
        this.federatedUris = federatedUris;
        entityURIs = ImmutableList.<String>builder()
                .addAll(federatedUris.entityURIs())
                .addAll(mainUris.entityURIs())
                .build();
        entityPrefixes = ImmutableMap.<String, String>builder()
                .putAll(federatedUris.entityPrefixes())
                .putAll(mainUris.entityPrefixes())
                .build();
        entityInitials = ImmutableList.<String>builder()
                .addAll(federatedUris.inlinableEntityInitials())
                .addAll(mainUris.inlinableEntityInitials())
                .build();
    }

    @Override
    public StringBuilder prefixes(StringBuilder query) {
        return mainUris.prefixes(query);
    }

    @Override
    public String root() {
        return mainUris.root();
    }

    @Override
    public String entityData() {
        return mainUris.entityData();
    }

    @Override
    public String entityDataHttps() {
        return mainUris.entityDataHttps();
    }

    @Override
    public String entityIdToURI(String entityId) {
        // FIXME: this is really fragile as entityInitials should only be used to detect inlinable URIs in blazegraph
        // Here we assume that mainUris always provides entityInitials that are "inline-able" by blazegraph.
        // By chance this only used by commons where MediaInfo entities are inline-able
        for (String initial : mainUris.inlinableEntityInitials()) {
            if (entityId.startsWith(initial)) {
                return mainUris.entityIdToURI(entityId);
            }
        }
        return federatedUris.entityIdToURI(entityId);
    }

    @Override
    public String entityURItoId(String uri) {
        if (mainUris.supportsUri(uri)) {
            return mainUris.entityURItoId(uri);
        }
        return federatedUris.entityURItoId(uri);
    }

    @Override
    public boolean isEntityURI(String uri) {
        if (mainUris.supportsUri(uri)) {
            return mainUris.isEntityURI(uri);
        }
        return federatedUris.isEntityURI(uri);
    }

    @Override
    public Collection<String> entityURIs() {
        return entityURIs;
    }

    @Override
    public Map<String, String> entityPrefixes() {
        return entityPrefixes;
    }

    @Override
    public Collection<String> inlinableEntityInitials() {
        return entityInitials;
    }

    @Override
    public String statement() {
        return mainUris.statement();
    }

    @Override
    public String value() {
        return mainUris.value();
    }

    @Override
    public String reference() {
        return mainUris.reference();
    }

    @Override
    public String property(PropertyType p) {
        return federatedUris.property(p);
    }

    public String property(String suffix) {
        return federatedUris.property(suffix);
    }

    @Override
    public String wellKnownBNodeIRIPrefix() {
        return mainUris.wellKnownBNodeIRIPrefix();
    }

    @Override
    public boolean supportsUri(String uri) {
        return mainUris.supportsUri(uri) || federatedUris.supportsUri(uri);
    }
}
