package org.wikidata.query.rdf.common.uri;

/**
 * Constants for <a href="http://www.w3.org/TR/REC-rdf-syntax/">RDF
 * primitives</a> and for the RDF namespace.
 */
public final class RDF {
    /**
     * Common prefix for all RDF predicates.
     */
    public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    /**
     * Predicate representing the type of a thing. Wikibase exports these
     * liberally and the Munge process removes many of them.
     */
    public static final String TYPE = NAMESPACE + "type";

    /**
     * Utility class uncallable constructor.
     */
    private RDF() {
        // Utility class.
    }
}
