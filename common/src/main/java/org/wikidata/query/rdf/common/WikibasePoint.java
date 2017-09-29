package org.wikidata.query.rdf.common;

import java.util.Locale;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Representation of a coordinate point in Wikibase.
 */
public class WikibasePoint {

    /**
     * Longitude, as string.
     */
    private final String longitude;
    /**
     * Latitude, as string.
     */
    private final String latitude;
    /**
     * Globe URI, as string.
     */
    private final String globe;

    /**
     * Coordinate order options.
     * Either latitude-longitude, or longitude-latitude.
     */
    @SuppressWarnings({"javadocvariable", "visibilitymodifier"})
    public enum CoordinateOrder {
        LAT_LONG, LONG_LAT;

        /**
         * Other option.
         */
        private CoordinateOrder other;

        /**
         * Other option.
         */
        public CoordinateOrder getOther() {
            return other;
        }

        static {
            LAT_LONG.other = LONG_LAT;
            LONG_LAT.other = LAT_LONG;
        }
    };

    /**
     * Default coordinate order in the system.
     * WKT requires long-lat order, but format 0.0.1 had lat-long.
     */
    public static final CoordinateOrder DEFAULT_ORDER = CoordinateOrder.LONG_LAT;

    /**
     * Get longitude.
     * @return Point's longitude.
     */
    public String getLongitude() {
        return longitude;
    }

    /**
     * Get latitude.
     * @return Point's latitude.
     */
    public String getLatitude() {
        return latitude;
    }

    /**
     * Get globe.
     * @return The globe.
     */
    public String getGlobe() {
        return globe;
    }

    /**
     * Create point from WKT literal.
     * @param literalString
     * @param coordOrder
     */
    public WikibasePoint(String literalString, CoordinateOrder coordOrder) {
        if (literalString.charAt(0) == '<') {
            // Extended OpenGIS format: "<URI> Point(1.2 3.4)"
            int endURI = literalString.indexOf('>');
            if (endURI <= 2) {
                throw new IllegalArgumentException("Invalid format for the WKT value");
            }
            globe = literalString.substring(1, endURI);
            literalString = literalString.substring(endURI + 2);
        } else {
            globe = null;
        }

        literalString = literalString.trim();
        if (!literalString.toLowerCase(Locale.ROOT).startsWith("point(") || !literalString.endsWith(")")) {
            throw new IllegalArgumentException("Invalid format for the WKT value");
        }

        // The point format is Point(123.45 -56.78)
        String[] coords = literalString.substring(6, literalString.length() - 1).split("[\\s,]");
        if (coords.length != 2) {
            throw new IllegalArgumentException("Invalid format for the WKT value");
        }
        if (coordOrder == CoordinateOrder.LAT_LONG) {
            latitude = coords[0];
            longitude = coords[1];
        } else {
            longitude = coords[0];
            latitude = coords[1];
        }
    }

    public WikibasePoint(String literalString) {
        this(literalString, DEFAULT_ORDER);
    }

    /**
     * Create point from array of strings.
     * @param components
     * @param globe
     * @param order
     */
    @SuppressFBWarnings(value = "CLI_CONSTANT_LIST_INDEX", justification = "array used as a pair")
    public WikibasePoint(String[] components, String globe, CoordinateOrder order) {
        if (order == CoordinateOrder.LAT_LONG) {
            latitude = components[0];
            longitude = components[1];
        } else {
            longitude = components[0];
            latitude = components[1];
        }
        this.globe = globe;
    }

    public WikibasePoint(String[] components, String globe) {
        this(components, globe, DEFAULT_ORDER);
    }

    public WikibasePoint(String[] components) {
        this(components, null, DEFAULT_ORDER);
    }

    public WikibasePoint(String[] components, CoordinateOrder order) {
        this(components, null, order);
    }

    /**
     * Get string representation in WKT format.
     */
    public String toString() {
        return toOrder(DEFAULT_ORDER);
    }

    /**
     * String representation in given coordinate order.
     * @param order
     * @return String representation.
     */
    public String toOrder(CoordinateOrder order) {
        final StringBuilder buf = new StringBuilder();
        if (globe != null) {
            buf.append('<');
            buf.append(globe);
            buf.append("> ");
        }
        buf.append("Point(");
        if (order == CoordinateOrder.LAT_LONG) {
            buf.append(latitude);
            buf.append(' ');
            buf.append(longitude);
        } else {
            buf.append(longitude);
            buf.append(' ');
            buf.append(latitude);
        }
        buf.append(')');
        return buf.toString();
    }
}
