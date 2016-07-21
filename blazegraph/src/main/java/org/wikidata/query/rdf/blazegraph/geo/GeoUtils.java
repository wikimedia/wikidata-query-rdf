package org.wikidata.query.rdf.blazegraph.geo;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.WikibasePoint.CoordinateOrder;
import org.wikidata.query.rdf.common.uri.GeoSparql;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.gis.CoordinateDD;

import static com.bigdata.rdf.internal.constraints.IVValueExpression.asLiteral;

/**
 * Miscellaneous useful functions for geospatial code.
 */
@SuppressWarnings("rawtypes")
public final class GeoUtils {

    /**
     * URI form of WKT literal type.
     */
    public static final URI WKT_LITERAL_URI = new URIImpl(GeoSparql.WKT_LITERAL);

    /**
     * Empty private ctor.
     */
    private GeoUtils() {

    }

    /**
     * Get point as a Literal.
     * @param point
     * @return Point as a literal.
     */
    public static Literal pointLiteral(String point) {
        return new LiteralImpl(point, WKT_LITERAL_URI);
    }

    /**
     * Create WikibasePoint from IV literal.
     *
     * @param iv
     * @return
     */
    public static WikibasePoint pointFromIV(IV iv) {
        return new WikibasePoint(asLiteral(iv).stringValue());
    }

    /**
     * Create CoordinateDD from WikibasePoint.
     *
     * @param point
     * @return
     */
    public static CoordinateDD getCoordinate(WikibasePoint point) {
        return new CoordinateDD(Double.parseDouble(point.getLatitude()),
                Double.parseDouble(point.getLongitude()));
    }

    /**
     * Create CoordinateDD from IV literal.
     *
     * @param iv
     * @return
     */
    public static CoordinateDD getCoordinate(IV iv) {
        return getCoordinate(pointFromIV(iv));
    }

    /**
     * Utility class for representing a coordinate box, given by two diagonal
     * corners.
     *
     * Any corners can be used as input, NE and SW corners are output.
     */
    public static class Box {
        /**
         * NE corner of the box.
         */
        private final WikibasePoint ne;
        /**
         * SW corner of the box.
         */
        private final WikibasePoint sw;
        /**
         * Switch mark. Did we use original coordinates or switched them?
         */
        private boolean switched;

        public Box(WikibasePoint east, WikibasePoint west) {
            final CoordinateDD eastPoint = getCoordinate(east);
            final CoordinateDD westPoint = getCoordinate(west);

            if (eastPoint.northSouth >= westPoint.northSouth) {
                // easy case - east point on the north
                ne = east;
                sw = west;
            } else {
                // need to swap
                ne = new WikibasePoint(
                        new String[]{east.getLongitude(), west.getLatitude()},
                        CoordinateOrder.LONG_LAT);
                sw = new WikibasePoint(
                        new String[]{west.getLongitude(), east.getLatitude()},
                        CoordinateOrder.LONG_LAT);
                switched = true;
            }
        }

        /**
         * Get NE corner.
         *
         * @return NE corner.
         */
        public WikibasePoint northEast() {
            return ne;
        }

        /**
         * Get SW corner.
         *
         * @return SW corner.
         */
        public WikibasePoint southWest() {
            return sw;
        }

        /**
         * Check if original coordinates were modified.
         *
         * @return Whether we switched?
         */
        public boolean switched() {
            return switched;
        }
    }

}
