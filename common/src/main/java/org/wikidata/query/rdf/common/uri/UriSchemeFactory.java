package org.wikidata.query.rdf.common.uri;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Factory class for system URI scheme.
 * Same instance will have only one URI scheme.
 */
public final class UriSchemeFactory {
    /**
     * A WikibaseUris instance for wikidata.org.
     */
    public static final WikibaseUris WIKIDATA = WikibaseUris.forHost("www.wikidata.org");
    /**
     * Configuration for wikibase host.
     */
    private static final String WIKIBASE_HOST_PROPERTY = "wikibaseHost";
    /**
     * Configuration for wikibase host.
     */
    private static final String WIKIBASE_CONCEPT_URI = "wikibaseConceptUri";
    /**
     * Configuration for Commons URI sceme - for SDC.
     */
    private static final String COMMONS_CONCEPT_URI = "commonsConceptUri";
    /**
     * Current URI system. This is static since each instance has only one URI
     * system.
     * This URI system is used for Blazegraph and tests. Tools construct their own URI system.
     */
    private static final WikibaseUris uriSystem = initializeURISystem();

    /**
     * Private ctor.
     */
    private UriSchemeFactory() {
    }

    /**
     * Return current URI system.
     *
     * @return Current URI system.
     */
    public static WikibaseUris getURISystem() {
        return uriSystem;
    }

    private static WikibaseUris initializeURISystem() {
        String wikibaseUriProperty = System.getProperty(WIKIBASE_CONCEPT_URI);
        if (wikibaseUriProperty != null) {
            return fromConceptUris(wikibaseUriProperty, System.getProperty(COMMONS_CONCEPT_URI));
        }
        String wikibaseHostProperty = System.getProperty(WIKIBASE_HOST_PROPERTY);
        if (wikibaseHostProperty != null) {
            return WikibaseUris.forHost(wikibaseHostProperty);
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
    public static WikibaseUris fromConceptUris(String wikibaseConceptUri, String commonsConceptUri) {
        try {
            if (commonsConceptUri != null) {
                return new SDCUris(new URI(commonsConceptUri), new URI(wikibaseConceptUri));
            } else {
                return new WikibaseUris(new URI(wikibaseConceptUri));
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException("Bad URI: " + wikibaseConceptUri + (commonsConceptUri != null ? ", " + commonsConceptUri : ""), e);
        }
    }

}
