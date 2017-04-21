package org.wikidata.query.rdf.blazegraph.constraints;

import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Implements the now() operator.
 */
public class WikibaseNowBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    /**
	 *
	 */
    private static final long serialVersionUID = 9136864442064392445L;

    /**
     * Ctor.
     * @param globals
     */
    public WikibaseNowBOp(final GlobalAnnotations globals) {

        this(BOp.NOARGS, anns(globals));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public WikibaseNowBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public WikibaseNowBOp(final WikibaseNowBOp op) {

        super(op);

    }

    /**
     * Get expression value.
     */
    public IV get(final IBindingSet bs) {

        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        WikibaseDate wd = WikibaseDate.fromSecondsSinceEpoch(cal.getTimeInMillis() / 1000);
        return super.asIV(getValueFactory().createLiteral(
                wd.toString(ToStringFormat.DATE_TIME),
                XMLSchema.DATETIME
               ), bs);
    }

     /**
      * Convert operation to string now().
      */
    public String toString() {

        return "now()";

    }

    /**
     * Never needs materialization.
     */
    public Requirement getRequirement() {
        return Requirement.NEVER;
    }

}

