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
     * geof: namespace.
     */
    public static final String FUNCTION_NAMESPACE = "http://www.opengis.net/def/geosparql/function/";

    /**
     * WKT literal type.
     */
    public static final String WKT_LITERAL = NAMESPACE + "wktLiteral";

    /**
     * Function name for function producing NE corner of the box, given any two diagonal corners.
     */
    public static final String NORTH_EAST_FUNCTION = FUNCTION_NAMESPACE + "northEast";

    /**
     * Function name for function producing SW corner of the box, given any two diagonal corners.
     */
    public static final String SOUTH_WEST_FUNCTION = FUNCTION_NAMESPACE + "southWest";

    /**
     * Function name for function producing globe from WKT literal.
     */
    public static final String GLOBE_FUNCTION = FUNCTION_NAMESPACE + "globe";
    /**
     * Function name for function producing longitude from WKT literal.
     */
    public static final String LON_FUNCTION = FUNCTION_NAMESPACE + "longitude";
    /**
     * Function name for function producing latitude from WKT literal.
     */
    public static final String LAT_FUNCTION = FUNCTION_NAMESPACE + "latitude";

    private GeoSparql() {
        // Utility uncallable constructor
    }
}
