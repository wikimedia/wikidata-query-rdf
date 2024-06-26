package org.wikidata.query.rdf.blazegraph.constraints;

import java.math.BigInteger;
import java.util.Map;

import org.wikidata.query.rdf.common.WikibaseDate;

import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.constraints.DateBOp;
import com.bigdata.rdf.internal.constraints.DateBOp.DateOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A date expression involving a left IValueExpression operand.
 * The operation to be applied to the operands is specified by the {@link Annotations}
 * annotation.
 * @see com.bigdata.rdf.internal.constraints.DateBOp
 * We are not extending com.bigdata.rdf.internal.constraints since get() is final there.
 */
public class WikibaseDateBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
     *
     */
    @SuppressFBWarnings(
                value = "IMC_IMMATURE_CLASS_BAD_SERIALVERSIONUID",
                justification = "We need to keep serialVersionUID for blazegraph correctness sake.")
    private static final long serialVersionUID = 9136864442064392445L;

    /**
     * Backup DateBOp for dates that aren't ours.
     */
    private final DateBOp originalOp;

    /**
     *
     * @param left  The left operand.
     * @param op    The annotation specifying the operation to be performed on those operands.
     */
    public WikibaseDateBOp(final IValueExpression<? extends IV> left,
            final DateOp op, final GlobalAnnotations globals) {

        this(new BOp[] {left}, anns(globals, new NV(DateBOp.Annotations.OP, op)));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param anns
     *            The operation.
     */
    public WikibaseDateBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null || getProperty(DateBOp.Annotations.OP) == null) {

            throw new IllegalArgumentException();

        }
        originalOp = new DateBOp(args, anns);
    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(BOp)}.
     */
    public WikibaseDateBOp(final WikibaseDateBOp op) {

        super(op);
        originalOp = new DateBOp(op.originalOp);
    }

    /**
     * Get Wikibase date from IV.
     * @param iv
     * @return Wikibase date object
     */
    @SuppressWarnings("rawtypes")
    private WikibaseDate getWikibaseDate(IV iv) {
        if (iv instanceof LiteralExtensionIV) {
            return WikibaseDate.fromSecondsSinceEpoch(((LiteralExtensionIV)iv).getDelegate().longValue());
        }
        try {
            return WikibaseDate.fromString(iv.getValue().stringValue()).cleanWeirdStuff();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Get expression value.
     */
    @SuppressWarnings({"rawtypes", "checkstyle:cyclomaticcomplexity"})
    public IV get(final IBindingSet bs) {

        final IV left = left().get(bs);

        // not yet bound?
        if (left == null) {
            throw new SparqlTypeErrorException.UnboundVarException();
        }

        if (left.isLiteral()) {
            BigdataLiteral bl = (BigdataLiteral) left.getValue();
            if (XSD.DATETIME.equals(bl.getDatatype())) {
                final WikibaseDate date = getWikibaseDate(left);
                if (date == null) {
                    throw new SparqlTypeErrorException();
                }

                switch (op()) {
                    case YEAR:
                        return new XSDIntegerIV(BigInteger.valueOf(date.year()));
                    case MONTH:
                        return new XSDIntegerIV(BigInteger.valueOf(date.month()));
                    case DAY:
                        return new XSDIntegerIV(BigInteger.valueOf(date.day()));
                    case HOURS:
                        return new XSDIntegerIV(BigInteger.valueOf(date.hour()));
                    case MINUTES:
                        return new XSDIntegerIV(BigInteger.valueOf(date.minute()));
                    case SECONDS:
                        return new XSDIntegerIV(BigInteger.valueOf(date.second()));
                    default:
                        throw new UnsupportedOperationException();
                }
            } else {
                return originalOp.get(bs);
            }
        }
        throw new SparqlTypeErrorException();
    }

    /**
     * Get left operand.
     */
    public IValueExpression<? extends IV> left() {
        return get(0);
    }

    /**
     * Get annotated operation.
     */
    public DateOp op() {
        return (DateOp) getRequiredProperty(DateBOp.Annotations.OP);
    }

    /**
     * Convert to string.
     */
    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(op());
        sb.append('(').append(left()).append(')');
        return sb.toString();

    }

    /**
     * Materialization requirements.
     */
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

}

