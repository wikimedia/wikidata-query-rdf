package org.wikidata.query.rdf.common.uri;

/**
 * Constants for the <a href="http://www.w3.org/TR/rdf-schema/">RDF Vocabulary
 * Description Language 1.0: RDF Schema</a> (RDFS).
 */
public final class RDFS {
    /**
     * Namespace for RDFS.
     */
    public static final String NAMESPACE = "http://www.w3.org/2000/01/rdf-schema#";
    /**
     * Wikibase dumps the label in this predicate, schema:name and
     * skos:prefLabel. We only keep labels with this predicate.
     */
    public static final String LABEL = NAMESPACE + "label";

    /**
     * Add the rdfs: prefix to the query.
     */
    public static StringBuilder prefixes(StringBuilder query) {
        return query.append("PREFIX rdfs: <").append(NAMESPACE).append(">\n");
    }

    /**
     * Utility class uncallable constructor.
     */
    private RDFS() {
        // Utility class.
    }
}
