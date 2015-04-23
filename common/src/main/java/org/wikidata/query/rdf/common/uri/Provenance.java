package org.wikidata.query.rdf.common.uri;

/**
 * URIs described on http://www.w3.org/ns/prov#.
 */
public final class Provenance {
    /**
     * Common prefix for all provenance predicates.
     */
    public static final String NAMESPACE = "http://www.w3.org/ns/prov#";
    /**
     * Declaration that a fact was derived from some source. Wikibase links
     * statements to references with this predicate.
     */
    public static final String WAS_DERIVED_FROM = NAMESPACE + "wasDerivedFrom";

    /**
     * Add the prov prefix to a query.
     */
    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX prov: <").append(NAMESPACE).append(">\n");
    }

    /**
     * Utility class uncallable constructor.
     */
    private Provenance() {
        // Utility class.
    }
}
