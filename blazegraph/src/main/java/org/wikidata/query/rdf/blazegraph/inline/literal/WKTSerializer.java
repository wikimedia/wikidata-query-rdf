package org.wikidata.query.rdf.blazegraph.inline.literal;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.WikibasePoint.CoordinateOrder;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.service.geospatial.GeoSpatialSearchException;
import com.bigdata.service.geospatial.IGeoSpatialLiteralSerializer;

/**
 * Serializer class for WKT format.
 * See https://portal.opengeospatial.org/files/?artifact_id=47664
 * and http://postgis.refractions.net/documentation/manual-1.3SVN/ch04.html
 *
 * Internal storage follows longitude-latitude order for this format.
 */
public class WKTSerializer implements IGeoSpatialLiteralSerializer {

    /**
     * Prefix for globe URIs.
     */
    private final String urlPrefix;

    /**
     * This is put in coordinate field when there's no globe.
     */
    public static final String NO_GLOBE = System.getProperty(
            WKTSerializer.class.getName() + ".noGlobe", "0");

    /**
     * URI of the wkt literal datatype.
     */
    private static final URI WKT_LITERAL_URI = new URIImpl(GeoSparql.WKT_LITERAL);

    public WKTSerializer() {
        // FIXME: we need a way to handle non-wikidata URIs
        urlPrefix = WikibaseUris.getURISystem().entity() + "Q";
    }

    @Override
    public String fromComponents(Object[] components) {
        if (components == null)
            return "";

        if (components.length != 3)
            throw new GeoSpatialSearchException(
                "Expected component string of lenth 2, but was " + components.length);

        String[] strComponents = new String[3];
        strComponents[0] = components[0].toString();
        strComponents[1] = components[1].toString();
        strComponents[2] = components[2].toString();

        WikibasePoint point;
        if (strComponents[2].equals(NO_GLOBE)) {
            point = new WikibasePoint(strComponents, null, CoordinateOrder.LONG_LAT);
        } else {
            point = new WikibasePoint(strComponents, urlPrefix + strComponents[2], CoordinateOrder.LONG_LAT);
        }

        return point.toString();
    }

    /**
     * Cut off prefix for the coordinate system URI.
     * E.g. http://www.wikidata.org/entity/Q
     * @param uri
     * @return
     */
    public String trimCoordURI(String uri) {
        if (!uri.startsWith(urlPrefix)) {
            throw new GeoSpatialSearchException("Invalid coordinate URI for the WKT value");
        }
        return uri.substring(urlPrefix.length());
    }

    @Override
    public String[] toComponents(String literalString) {
        if (literalString == null) {
            return new String[0];
        }

        String[] comps = new String[3];
        WikibasePoint point = new WikibasePoint(literalString);
        // Should be in sync with RWStore.properties config
        // and AbstractRandomizedBlazegraphStorageTestCase.java
        comps[0] = point.getLongitude();
        comps[1] = point.getLatitude();
        String globe = point.getGlobe();
        comps[2] = globe != null ? trimCoordURI(globe) : NO_GLOBE;

        return comps;
    }

    @Override
    public IV<?, ?> serializeCoordSystem(BigdataValueFactory vf, Object coordinateSystem) {
        // Returns URI with prefix, so you can match it with entities
        return DummyConstantNode.toDummyIV(vf.createURI(urlPrefix + coordinateSystem.toString()));
    }

    @Override
    public IV<?, ?> serializeCustomFields(BigdataValueFactory vf, Object... arg1) {
        throw new IllegalArgumentException("Custom fields are not supported for this format");
    }

    @Override
    public IV<?, ?> serializeLatitude(BigdataValueFactory vf, Object latitude) {
        return new XSDNumericIV((Double)latitude);
    }

    @Override
    public IV<?, ?> serializeLocation(BigdataValueFactory vf, Object lat, Object lon) {
        // FIXME: this does not look really useful. Maybe needs to produce exception too.
        WikibasePoint p = new WikibasePoint(new String[] {lat.toString(), lon.toString()},
                null, CoordinateOrder.LAT_LONG);
        return DummyConstantNode.toDummyIV(vf.createLiteral(p.toString(), WKT_LITERAL_URI));
    }

    @Override
    public IV<?, ?> serializeLocationAndTime(BigdataValueFactory arg0,
            Object arg1, Object arg2, Object arg3) {
        throw new IllegalArgumentException("Time fields are not supported for this format");
    }

    @Override
    public IV<?, ?> serializeLongitude(BigdataValueFactory vf, Object longitude) {
        return new XSDNumericIV((Double)longitude);
    }

    @Override
    public IV<?, ?> serializeTime(BigdataValueFactory arg0, Object arg1) {
        throw new IllegalArgumentException("Time fields are not supported for this format");
    }

}
