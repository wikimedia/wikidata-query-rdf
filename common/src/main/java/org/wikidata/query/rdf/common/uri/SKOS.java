package org.wikidata.query.rdf.common.uri;

/**
 * Used for labels mostly.
 */
public class SKOS {
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
}
