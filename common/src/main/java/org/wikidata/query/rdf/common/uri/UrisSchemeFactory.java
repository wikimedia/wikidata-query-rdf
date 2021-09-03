package org.wikidata.query.rdf.common.uri;

import static java.util.Collections.singletonList;
import static org.wikidata.query.rdf.common.uri.UrisConstants.COMMONS_CONCEPT_URI;
import static org.wikidata.query.rdf.common.uri.UrisConstants.MEDIAINFO_INITIAL;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_CONCEPT_URI;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_HOST_PROPERTY;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_INITIALS;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Factory class for system URI scheme.
 * Same instance will have only one URI scheme.
 */
public final class UrisSchemeFactory {
    /**
     * A WikibaseUris instance for wikidata.org.
     */
    public static final UrisScheme WIKIDATA = forHost("www.wikidata.org");
    /**
     * Current URI system. This is static since each instance has only one URI
     * system.
     */
    private static final UrisScheme uriSystem = initializeURISystem();

    /**
     * Private ctor.
     */
    private UrisSchemeFactory() {
    }

    /**
     * Return current URI system.
     */
    public static UrisScheme getURISystem() {
        return uriSystem;
    }

    private static UrisScheme initializeURISystem() {
        String wikibaseUriProperty = System.getProperty(WIKIBASE_CONCEPT_URI);
        if (wikibaseUriProperty != null) {
            return fromConceptUris(wikibaseUriProperty, System.getProperty(COMMONS_CONCEPT_URI));
        }
        String wikibaseHostProperty = System.getProperty(WIKIBASE_HOST_PROPERTY);
        if (wikibaseHostProperty != null) {
            return forHost(wikibaseHostProperty);
        } else {
            return WIKIDATA;
        }
    }

    /**
     * Create URI scheme from pair of concept URIs.
     * @param wikibaseConceptUri Wikibase URI
     * @param commonsConceptUri Commons URI, can be NULL if we're not dealing with SDC
     * @return URI scheme
     */
    public static UrisScheme fromConceptUris(@Nonnull String wikibaseConceptUri, @Nullable String commonsConceptUri) {
        try {
            if (commonsConceptUri != null) {
                UrisScheme sdcUris = new DefaultUrisScheme(
                        new URI(commonsConceptUri),
                        SDC_ENTITY_PREFIX, SDC_ENTITY_DATA_PREFIX, singletonList(MEDIAINFO_INITIAL));
                UrisScheme wikibaseUris = new DefaultUrisScheme(
                        new URI(wikibaseConceptUri),
                        WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
                return new FederatedUrisScheme(sdcUris, wikibaseUris);
            } else {
                return new DefaultUrisScheme(
                        new URI(wikibaseConceptUri),
                        WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Bad URI: " + wikibaseConceptUri + (commonsConceptUri != null ? ", " + commonsConceptUri : ""), e);
        }
    }


    /**
     * Build for a specific wikibase host. See the WIKIDATA constant for how you
     * can use this.
     */
    public static UrisScheme forHost(@Nonnull String host) {
        try {
            return new DefaultUrisScheme(new URI("http://" + host),
                    WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Bad URI host: " + host, e);
        }
    }
}
