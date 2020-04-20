package org.wikidata.query.rdf.blazegraph.constraints;

import static org.wikidata.query.rdf.blazegraph.geo.GeoUtils.pointFromIV;

import java.util.Map;

import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.blazegraph.geo.GeoUtils;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.gis.CoordinateDD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Get NE or SW corners of the box, given two arbitrary diagonal corners.
 */
public class WikibaseCornerBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
     * Corners supported by this op.
     */
    public enum Corners { NE, SW }

    /**
     * Annotaion for specific corner.
     * The operation to be applied to the operands (required).
     * The value of this annotation is a {@link WikibaseCornerBOp.Corners}.
     */
    private static final String OP_ANNOTATION = (WikibaseCornerBOp.class.getName() + ".op").intern();

    /**
     * Required shallow copy constructor.
     */
    public WikibaseCornerBOp(final BOp[] args,
            final Map<String, Object> anns) {
        super(args, anns);

        if (args.length < 2 || args[0] == null || args[1] == null)
            throw new IllegalArgumentException();
    }

    /**
     * Main ctor.
     * @param left Eastern corner of the box
     * @param right Wester corner of the box
     * @param corner Which corner we want as the result
     * @param globals  The global annotations, including the lexicon namespace
     */
    @SuppressWarnings("rawtypes")
    public WikibaseCornerBOp(final IValueExpression<? extends IV> left,
            final IValueExpression<? extends IV> right,
            final Corners corner,
            final GlobalAnnotations globals) {
        this(new BOp[]{left, right},
                anns(globals, new NV(OP_ANNOTATION, corner)));
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(BOp)})}.
     */
    public WikibaseCornerBOp(final WikibaseCornerBOp op) {
        super(op);
    }

    /**
     * Get which corner we're needing for this op.
     * @return The corner for this specific op.
     */
    private Corners corner() {
        return (Corners) getRequiredProperty(OP_ANNOTATION);
    }

    /**
     * Get coordinate from WikibasePoint value.
     *
     * @return Coordinate
     */
    protected CoordinateDD getCoordinateFromWP(WikibasePoint point) {
        return new CoordinateDD(Double.parseDouble(point.getLatitude()),
                                Double.parseDouble(point.getLongitude()));
    }

    @Override
    public IV get(IBindingSet bindingSet) {
        final IV east = getAndCheckLiteral(0, bindingSet);
        final IV west = getAndCheckLiteral(1, bindingSet);

        final GeoUtils.Box box = new GeoUtils.Box(pointFromIV(east), pointFromIV(west));

        WikibasePoint wp;

        if (corner() == Corners.NE) {
            if (!box.switched()) {
                return east;
            }
            wp = box.northEast();
        } else {
            if (!box.switched()) {
                return west;
            }
            wp = box.southWest();
        }

        final BigdataLiteral newpoint = getValueFactory().createLiteral(
                wp.toString(), new URIImpl(GeoSparql.WKT_LITERAL));

        return super.asIV(newpoint, bindingSet);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.ALWAYS;
    }

}
