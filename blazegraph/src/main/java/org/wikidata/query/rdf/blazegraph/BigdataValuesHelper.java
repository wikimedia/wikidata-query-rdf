package org.wikidata.query.rdf.blazegraph;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Helper class to create Bigdata values from constants.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class BigdataValuesHelper {

    /**
     * Hidden ctor.
     */
    private BigdataValuesHelper() {}

    /**
     * Create IV from constant and type.
     */
    public static IV makeIV(BigdataValueFactory vf, String value, URI type) {
        return makeIV(vf, vf.createLiteral(value, type));
    }

    /**
     * Make IV from a literal.
     */
    public static IV makeIV(BigdataValueFactory vf, Literal literal) {
        return makeIV(vf, literal, VTE.LITERAL);
    }

    /**
     * Make IV for URI.
     */
    public static IV makeIV(BigdataValueFactory vf, URI uriString) {
        return makeIV(vf, vf.createURI(uriString.stringValue()), VTE.URI);
    }

    /**
     * Make IV from a value.
     * @param type Value type.
     */
    public static IV makeIV(BigdataValueFactory vf, Value value, VTE type) {
        final BigdataValue l = vf.asValue(value);
        TermId mock = TermId.mockIV(type);
        mock.setValue(l);
        l.setIV(mock);
        return mock;
    }

    /**
     * Create IV from string constant.
     */
    public static IV makeIV(BigdataValueFactory vf, String value) {
        return makeIV(vf, vf.createLiteral(value));
    }

    /**
     * Create Constant from string.
     */
    public static IConstant makeConstant(BigdataValueFactory vf, String value) {
        return new Constant(makeIV(vf, value));
    }

    /**
     * Create Constant from URI.
     */
    public static IConstant makeConstant(BigdataValueFactory vf, URI value) {
        return new Constant(makeIV(vf, value));
    }

    /**
     * Create Constant from string and type.
     */
    public static IConstant makeConstant(BigdataValueFactory vf, String value, URI type) {
        return new Constant(makeIV(vf, value, type));
    }

    /**
     * Create constant from int.
     */
    @SuppressFBWarnings(value = "UP_UNUSED_PARAMETER", justification = "Don't need BigdataValueFactory, but leave it to have uniform API")
    public static IConstant makeConstant(BigdataValueFactory vf, int value) {
        return new Constant(new XSDNumericIV<>(value));
    }

    /**
     * Create constant from double.
     */
    @SuppressFBWarnings(value = "UP_UNUSED_PARAMETER", justification = "Don't need BigdataValueFactory, but leave it to have uniform API")
    public static IConstant makeConstant(BigdataValueFactory vf, double value) {
        return new Constant(new XSDNumericIV<>(value));
    }

    /**
     * Create new variable with given name.
     */
    public static IVariable makeVariable(String varName) {
        return Var.var(varName);
    }

}
