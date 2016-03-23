package org.wikidata.query.rdf.common.uri;

/**
 * GeoSPARQL URIs.
 */
public final class GeoSparql {
    /**
     * geo: namespace.
     */
    public static final String NAMESPACE = "http://www.opengis.net/ont/geosparql#";

    /**
     * WKT literal type.
     */
    public static final String WKT_LITERAL = NAMESPACE + "wktLiteral";

    private GeoSparql() {
        // Utility uncallable constructor
    }
}
