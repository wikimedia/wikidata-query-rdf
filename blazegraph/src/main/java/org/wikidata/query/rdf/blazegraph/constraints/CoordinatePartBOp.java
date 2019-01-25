package org.wikidata.query.rdf.blazegraph.constraints;

import static org.wikidata.query.rdf.blazegraph.geo.GeoUtils.pointFromIV;

import java.util.Map;

import org.wikidata.query.rdf.common.WikibasePoint;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Get parts of coordinate.
 */
public class CoordinatePartBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
     *
     */
    private static final long serialVersionUID = -81134263515935773L;

    /**
     * Parts supported by this op.
     */
    public enum Parts { GLOBE, LAT, LON }

    /**
     * Annotation for specific part.
     * The operation to be applied to the operands (required).
     * The value of this annotation is a {@link CoordinatePartBOp.Parts}.
     */
    private static final String OP_ANNOTATION = (CoordinatePartBOp.class.getName() + ".op").intern();

    /**
     * Required shallow copy constructor.
     */
    public CoordinatePartBOp(final BOp[] args,
            final Map<String, Object> anns) {
        super(args, anns);

        if (args.length < 1 || args[0] == null)
            throw new IllegalArgumentException();
    }

    /**
     * Main ctor.
     * @param coord Coordinate
     * @param part Which part we need to get
     * @param globals The global annotations, including the lexicon namespace
     */
    @SuppressWarnings("rawtypes")
    public CoordinatePartBOp(final IValueExpression<? extends IV> coord,
            final Parts part,
            final GlobalAnnotations globals) {
        this(new BOp[]{coord},
                anns(globals, new NV(OP_ANNOTATION, part)));
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(BOp)}.
     */
    public CoordinatePartBOp(final CoordinatePartBOp op) {
        super(op);
    }

    /**
     * Get which part we're needing for this op.
     * @return Part which we're getting with this op
     */
    private Parts part() {
        return (Parts) getRequiredProperty(OP_ANNOTATION);
    }

    @Override
    public IV get(IBindingSet bindingSet) {
        final IV coord = getAndCheckLiteral(0, bindingSet);
        final WikibasePoint point = pointFromIV(coord);
        final BigdataValue result;

        switch (part()) {
        case GLOBE:
            String globe = point.getGlobe();
            if (globe == null) {
                result = getValueFactory().createLiteral("");
            } else {
                result = getValueFactory().createURI(point.getGlobe());
            }
            break;
        case LON:
            result = getValueFactory().createLiteral(Double.parseDouble(point.getLongitude()));
            break;
        case LAT:
            result = getValueFactory().createLiteral(Double.parseDouble(point.getLatitude()));
            break;
        default:
            throw new IllegalArgumentException("Unknown part specified");
        }

        return super.asIV(result, bindingSet);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.ALWAYS;
    }

}
