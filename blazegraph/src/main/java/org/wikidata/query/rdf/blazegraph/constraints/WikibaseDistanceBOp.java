package org.wikidata.query.rdf.blazegraph.constraints;

import java.util.Map;

import org.wikidata.query.rdf.common.WikibasePoint;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.internal.gis.ICoordinate.UNITS;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

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
        final IV left = get(0).get(bindingSet);
        final IV right = get(1).get(bindingSet);

        final CoordinateDD leftPoint = getCoordinateFromIV(left);
        final CoordinateDD rightPoint = getCoordinateFromIV(right);
        // TODO: allow to supply Units
        final double distance = leftPoint.distance(rightPoint, UNITS.Kilometers);

        final BigdataLiteral dist = getValueFactory().createLiteral(distance);
        return super.asIV(dist, bindingSet);
    }

    /**
     * Get coordinate from IV value.
     * @param iv
     * @return Coordinate
     */
    protected CoordinateDD getCoordinateFromIV(IV iv) {
        final WikibasePoint point = new WikibasePoint(asLiteral(iv).stringValue());
        return new CoordinateDD(Double.parseDouble(point.getLatitude()),
                                Double.parseDouble(point.getLongitude()));
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.ALWAYS;
    }

}
