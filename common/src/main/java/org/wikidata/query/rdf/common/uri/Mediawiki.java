package org.wikidata.query.rdf.common.uri;

/**
 * Mediawiki ontology.
 */
public final class Mediawiki {
    /**
     * Common prefix of all ontology parts.
     */
    public static final String NAMESPACE = "https://www.mediawiki.org/ontology#";
    /**
     * Category class.
     */
    public static final String CATEGORY = NAMESPACE + "Category";
    /**
     * mediawiki:isInCategory predicate.
     */
    public static final String IS_IN_CATEGORY = NAMESPACE + "isInCategory";
    /**
     * mwapi: Mediawiki API prefix.
     */
    public static final String API = NAMESPACE + "API/";

    private Mediawiki() {
        // Utility class.
    }
}
