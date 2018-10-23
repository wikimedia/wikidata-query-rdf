package org.wikidata.query.rdf.common.uri;

/**
 * Used for labels mostly.
 */
public final class SKOS {
    /**
     * Common prefix for all skos predicates.
     */
    public static final String NAMESPACE = "http://www.w3.org/2004/02/skos/core#";
    /**
     * Wikibase dumps the label in this, schema:name and rdfs:label. We only
     * keep rdfs:label.
     */
    public static final String PREF_LABEL = NAMESPACE + "prefLabel";
    /**
     * Wikibase dumps the aliases in this.
     */
    public static final String ALT_LABEL = NAMESPACE + "altLabel";
    /**
     * Used for Lexeme senses.
     */
    public static final String DEFINITION = NAMESPACE + "definition";

    /**
     * Adds the skos: prefix to the query.
     */
    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX skos: <").append(NAMESPACE).append(">\n");
    }

    /**
     * Utility class uncallable constructor.
     */
    private SKOS() {
        // Utility class.
    }
}
