package org.wikidata.query.rdf.blazegraph.constraints;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.IVValueExpression;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implementation of URI decoder function.
 */
public class DecodeUriBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    @SuppressFBWarnings(
                value = "IMC_IMMATURE_CLASS_BAD_SERIALVERSIONUID",
                justification = "We need to keep serialVersionUID for blazegraph correctness sake.")
    private static final long serialVersionUID = -8448763718374010166L;

    public DecodeUriBOp(final IValueExpression<? extends IV> x,
            final GlobalAnnotations globals) {
        super(x, globals);
    }

    public DecodeUriBOp(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();
    }

    public DecodeUriBOp(DecodeUriBOp op) {
        super(op);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }

    @Override
    @SuppressFBWarnings(value = "LEST_LOST_EXCEPTION_STACK_TRACE", justification = "SparqlTypeErrorException does not allow setting a cause")
    public IV get(final IBindingSet bs) {
        final Literal lit = getAndCheckLiteralValue(0, bs);
        try {
            final BigdataLiteral str = getValueFactory().createLiteral(URLDecoder.decode(lit.getLabel(), UTF_8.name()));
            return super.asIV(str, bs);
        } catch (UnsupportedEncodingException uee) {
            throw new SparqlTypeErrorException();
        }
    }
}
