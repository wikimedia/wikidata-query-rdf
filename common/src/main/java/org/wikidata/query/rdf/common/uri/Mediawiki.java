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
     * Hidden Category class.
     */
    public static final String HIDDEN_CATEGORY = NAMESPACE + "HiddenCategory";
    /**
     * mediawiki:isInCategory predicate.
     */
    public static final String IS_IN_CATEGORY = NAMESPACE + "isInCategory";
    /**
     * Pages count predicate.
     */
    public static final String PAGES = NAMESPACE + "pages";
    /**
     * Pages count predicate.
     */
    public static final String SUBCATEGORIES = NAMESPACE + "subcategories";
    /**
     * mwapi: Mediawiki API prefix.
     */
    public static final String API = NAMESPACE + "API/";

    private Mediawiki() {
        // Utility class.
    }
}
