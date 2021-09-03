package org.wikidata.query.rdf.common.uri;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Holds constants and default values for the construction of {@link UrisScheme}s.
 *
 * While it would make some sense to move those constants to
 * {@link UrisSchemeFactory}, initialization order of static fields is more
 * explicit this way, so we avoid issues that are complex to debug.
 */
public final class UrisConstants {

    /**
     * Prefix for entity URI.
     */
    public static final String SDC_ENTITY_PREFIX = "sdc";
    /**
     * Prefix for entity data URI.
     */
    public static final String SDC_ENTITY_DATA_PREFIX = "sdcdata";
    /**
     * Initial letter for mediainfo items.
     */
    public static final String MEDIAINFO_INITIAL = "M";
    /**
     * Prefix for entity URI.
     */
    public static final String WIKIBASE_ENTITY_PREFIX = "wd";
    /**
     * Prefix for entity data URI.
     */
    public static final String WIKIBASE_ENTITY_DATA_PREFIX = "wdata";
    /**
     * Initial letters of supported Wikibase items.
     * L is not included because L IDs can not be inlined due
     * to conflict between Lexemes, Forms and Senses.
     * The order must be: P, Q for BC. See https://phabricator.wikimedia.org/T230588
     */
    public static final List<String> WIKIBASE_INITIALS = ImmutableList.of("P", "Q");
    /**
     * Configuration for wikibase host.
     */
    static final String WIKIBASE_HOST_PROPERTY = "wikibaseHost";
    /**
     * Configuration for wikibase host.
     */
    static final String WIKIBASE_CONCEPT_URI = "wikibaseConceptUri";
    /**
     * Configuration for Commons URI sceme - for SDC.
     */
    static final String COMMONS_CONCEPT_URI = "commonsConceptUri";

    private UrisConstants() {
        // Should never be constructed
    }
}
