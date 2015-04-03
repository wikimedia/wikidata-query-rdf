package org.wikidata.query.rdf.common.uri;

/**
 * Marks the kinds of things (items or properties).
 */
public class Ontology {
    public static final String NAMESPACE = "http://www.wikidata.org/ontology#";

    /**
     * Wikibase exports all items with an assertion that their RDF.TYPE is this
     * and we filter that out.
     */
    public static final String ITEM = NAMESPACE + "Item";
    /**
     * Wikibase exports all statements with an assertion that their RDF.TYPE is
     * this and we filter that out.
     */
    public static final String STATEMENT = NAMESPACE + "Statement";
    /**
     * Wikibase exports references with an assertion that their RDF.TYPE is this
     * and we fitler that out.
     */
    public static final String REFERENCE = NAMESPACE + "Reference";

    /**
     * Predicate for marking Wikibase's Rank.
     *
     * @see http://www.wikidata.org/wiki/Help:Ranking
     */
    public static final String RANK = NAMESPACE + "rank";
    public static final String BEST_RANK = NAMESPACE + "BestRank";
    public static final String PREFERRED_RANK = NAMESPACE + "PreferredRank";
    public static final String NORMAL_RANK = NAMESPACE + "NormalRank";
    public static final String DEPRECATED_RANK = NAMESPACE + "DeprecatedRank";

    public static final String VALUE = NAMESPACE + "Value";

    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX ontology: <").append(NAMESPACE).append(">\n");
    }
}
