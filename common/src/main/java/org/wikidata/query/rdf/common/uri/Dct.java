package org.wikidata.query.rdf.common.uri;

/**
 * DCMI Metadata Terms.
 */
public final class Dct {
    /**
     * dct: namespace.
     */
    public static final String NAMESPACE = "http://purl.org/dc/terms/";

    /**
     * Language predicate.
     * For linking Forms to language items.
     */
    public static final String LANGUAGE = NAMESPACE + "language";

    private Dct() {
        // Utility uncallable constructor
    }
}
