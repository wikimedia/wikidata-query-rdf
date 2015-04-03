package org.wikidata.query.rdf.common.uri;

/**
 * URIs described on http://www.w3.org/ns/prov#.
 */
public class Provenance {
    public static final String NAMESPACE = "http://www.w3.org/ns/prov#";
    public static final String WAS_DERIVED_FROM = NAMESPACE + "wasDerivedFrom";

    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX prov: <").append(NAMESPACE).append(">\n");
    }
}
