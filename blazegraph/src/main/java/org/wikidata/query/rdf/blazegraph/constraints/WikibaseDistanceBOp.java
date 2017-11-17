package org.wikidata.query.rdf.blazegraph.constraints;

import java.util.Map;

import org.wikidata.query.rdf.common.WikibasePoint;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implementation of geof:distance function.
 * FIXME: units not supported yet.
 */
@SuppressWarnings("rawtypes")
public class WikibaseDistanceBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
     *
     */
    private static final long serialVersionUID = 2909300288279424402L;

    /**
     * Required shallow copy constructor.
     */
    public WikibaseDistanceBOp(final BOp[] args,
            final Map<String, Object> anns) {
        super(args, anns);

        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();
    }

    @SuppressWarnings("rawtypes")
    public WikibaseDistanceBOp(//
            final IValueExpression<? extends IV> left,
            final IValueExpression<? extends IV> right,
            final GlobalAnnotations globals) {
        this(new BOp[] {left, right}, anns(globals));

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public WikibaseDistanceBOp(final WikibaseDistanceBOp op) {
        super(op);
    }

    @Override
    public IV get(IBindingSet bindingSet) {
        final IV left = getAndCheckLiteral(0, bindingSet);
        final IV right = getAndCheckLiteral(1, bindingSet);

        final CoordinateDD leftPoint = getCoordinateFromIV(left);
        final CoordinateDD rightPoint = getCoordinateFromIV(right);
        // TODO: allow to supply Units
        final double distance;
        if (leftPoint.equals(rightPoint) || veryClose(leftPoint, rightPoint)) {
            distance = 0;
        } else {
            distance = leftPoint.distance(rightPoint, UNITS.Kilometers);
        }

        final BigdataLiteral dist = getValueFactory().createLiteral(distance);
        return super.asIV(dist, bindingSet);
    }

    /**
     * Distance where the points are considered to be the same.
     * Distance equivalent of degrees (on equator):
     * 0.001   is about 110 m
     * 0.0001  is about 11 m
     * 0.00001 is about 1 m
     * See: https://en.wikipedia.org/wiki/WP:OPCOORD
     */
    public static final double SMALL_DISTANCE = 0.00001;

    /**
     * Check whether two points are very close to each other, so distance between them
     * can be considered zero.
     * Current CoordinateDD.distance does not work well with small distances,
     * so we take a shortcut here in order not to get the NaN.
     * @param p1
     * @param p2
     * @return Whether the points are very close.
     */
    protected boolean veryClose(CoordinateDD p1, CoordinateDD p2) {
        double dLon = p1.eastWest - p2.eastWest;
        double dLat = p1.northSouth - p2.northSouth;
        return Math.abs(dLon) < SMALL_DISTANCE && Math.abs(dLat) < SMALL_DISTANCE;
    }

    /**
     * Get coordinate from IV value.
     * @param iv
     * @return Coordinate
     */
    @SuppressFBWarnings(value = "LEST_LOST_EXCEPTION_STACK_TRACE", justification = "Converting to SPARQL exception")
    protected CoordinateDD getCoordinateFromIV(IV iv) {
        final WikibasePoint point = new WikibasePoint(asLiteral(iv).stringValue());
        try {
            return new CoordinateDD(Double.parseDouble(point.getLatitude()),
                                Double.parseDouble(point.getLongitude()));
        } catch (IllegalArgumentException e) {
            throw new SparqlTypeErrorException();
        }
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.ALWAYS;
    }

}
