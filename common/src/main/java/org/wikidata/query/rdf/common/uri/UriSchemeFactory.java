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
            String commonsUriProperty = System.getProperty(COMMONS_CONCEPT_URI);
            try {
                if (commonsUriProperty != null) {
                    return new SDCUris(new URI(commonsUriProperty), new URI(wikibaseUriProperty));
                } else {
                    return new WikibaseUris(new URI(wikibaseUriProperty));
                }
            } catch (URISyntaxException e) {
                throw new RuntimeException("Bad URI: " + wikibaseUriProperty + (commonsUriProperty != null ? ", " + commonsUriProperty : ""), e);
            }
        }
        String wikibaseHostProperty = System.getProperty(WIKIBASE_HOST_PROPERTY);
        if (wikibaseHostProperty != null) {
            return WikibaseUris.forHost(wikibaseHostProperty);
        } else {
            return WIKIDATA;
        }
    }
}
