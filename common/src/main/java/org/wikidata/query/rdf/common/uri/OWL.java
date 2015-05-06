package org.wikidata.query.rdf.common.uri;

/**
 * Representation of the OWL ontology.
 */
public final class OWL {
    /**
     * Common prefix for all OWL predicates.
     */
    public static final String NAMESPACE = "http://www.w3.org/2002/07/owl#";
    /**
     * owl:sameAs used for redirects.
     */
    public static final String SAME_AS = NAMESPACE + "sameAs";

    /**
     * owl:Class is an OWL class declaration.
     */
    public static final String CLASS = NAMESPACE + "Class";
    /**
     * Utility class uncallable constructor.
     */
    private OWL() {
        // Utility class.
    }
}
