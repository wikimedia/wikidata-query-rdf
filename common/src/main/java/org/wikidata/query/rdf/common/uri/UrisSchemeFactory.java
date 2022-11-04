package org.wikidata.query.rdf.common.uri;

import static java.util.Collections.singletonList;
import static org.wikidata.query.rdf.common.uri.UrisConstants.COMMONS_CONCEPT_URI_PROPERTY;
import static org.wikidata.query.rdf.common.uri.UrisConstants.MEDIAINFO_INITIAL;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_CONCEPT_URI_PROPERTY;
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
    public static final String WIKIDATA_HOSTNAME = "www.wikidata.org";
    public static final String COMMONS_HOSTNAME = "commons.wikimedia.org";

    /**
     * A WikibaseUris instance for wikidata.org.
     */
    public static final DefaultUrisScheme WIKIDATA = forWikidataHost(WIKIDATA_HOSTNAME);

    /**
     * A UrisScheme instance for commons.wikimedia.org. Supports
     * federation to wikidata.
     */
    public static final UrisScheme COMMONS = new FederatedUrisScheme(forCommonsHost(COMMONS_HOSTNAME), WIKIDATA);

    /**
     * Current URI system. This is static since each instance has only one URI
     * system.
     */
    private static final UrisScheme URI_SYSTEM = initializeURISystem();

    /**
     * Private ctor.
     */
    private UrisSchemeFactory() {
    }

    /**
     * Return current URI system.
     */
    public static UrisScheme getURISystem() {
        return URI_SYSTEM;
    }

    private static UrisScheme initializeURISystem() {
        // The strings say wikibase for historical reasons, but this means wikidata.
        String wikibaseUriProperty = System.getProperty(WIKIBASE_CONCEPT_URI_PROPERTY);
        if (wikibaseUriProperty != null) {
            return fromConceptUris(wikibaseUriProperty, System.getProperty(COMMONS_CONCEPT_URI_PROPERTY));
        }
        String wikibaseHostProperty = System.getProperty(WIKIBASE_HOST_PROPERTY);
        if (wikibaseHostProperty != null) {
            return forWikidataHost(wikibaseHostProperty);
        } else {
            return WIKIDATA;
        }
    }

    /**
     * Create URI scheme from pair of concept URIs.
     * @param wikidataConceptUri Wikidata URI
     * @param commonsConceptUri Commons URI, can be NULL if we're not dealing with SDC
     * @return URI scheme
     */
    public static UrisScheme fromConceptUris(@Nonnull String wikidataConceptUri, @Nullable String commonsConceptUri) {
        try {
            UrisScheme wikidataUris = forWikidata(new URI(wikidataConceptUri));
            if (commonsConceptUri != null) {
                DefaultUrisScheme sdcUris = forCommons(new URI(commonsConceptUri));
                return new FederatedUrisScheme(sdcUris, wikidataUris);
            } else {
                return wikidataUris;
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Bad URI: " + wikidataConceptUri + (commonsConceptUri != null ? ", " + commonsConceptUri : ""), e);
        }
    }


    /**
     * Build for a specific wikidata host.
     */
    public static DefaultUrisScheme forWikidataHost(@Nonnull String host) {
        return forWikidata(wikidataUri(host));
    }

    /**
     * Build for a specific commons host. Does not build a complete
     * scheme, commons also requires federation to wikidata. See
     * self::COMMONS for usage.
     */
    protected static DefaultUrisScheme forCommonsHost(@Nonnull String host) {
        return forCommons(commonsUri(host));
    }

    public static DefaultUrisScheme forWikidata(@Nonnull URI uri) {
        return new DefaultUrisScheme(uri, WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
    }

    public static DefaultUrisScheme forCommons(@Nonnull URI uri) {
        return new DefaultUrisScheme(uri, SDC_ENTITY_PREFIX, SDC_ENTITY_DATA_PREFIX, singletonList(MEDIAINFO_INITIAL));
    }

    public static URI commonsUri(String host) {
        return URI.create("https://" + host);
    }

    public static URI wikidataUri(String host) {
        return URI.create("http://" + host);
    }
}
