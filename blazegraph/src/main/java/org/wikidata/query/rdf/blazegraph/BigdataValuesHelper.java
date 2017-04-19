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
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

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
     * @param vf
     * @param value
     * @param type
     * @return
     */
    public static IV makeIV(BigdataValueFactory vf, String value, URI type) {
        return makeIV(vf, vf.createLiteral(value, type));
    }

    /**
     * Make IV from a literal.
     * @param vf
     * @param literal
     * @return
     */
    public static IV makeIV(BigdataValueFactory vf, Literal literal) {
        return makeIV(vf, literal, VTE.LITERAL);
    }

    /**
     * Make IV for URI.
     * @param vf
     * @param literal
     * @return
     */
    public static IV makeIV(BigdataValueFactory vf, URI uriString) {
        return makeIV(vf, vf.createURI(uriString.stringValue()), VTE.URI);
    }

    /**
     * Make IV from a value.
     * @param vf
     * @param value
     * @param type Value type.
     * @return
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
     * @param vf
     * @param value
     * @return
     */
    public static IV makeIV(BigdataValueFactory vf, String value) {
        return makeIV(vf, vf.createLiteral(value));
    }

    /**
     * Create Constant from string.
     * @param vf
     * @param value
     * @return
     */
    public static IConstant makeConstant(BigdataValueFactory vf, String value) {
        return new Constant(makeIV(vf, value));
    }

    /**
     * Create Constant from URI.
     * @param vf
     * @param value
     * @return
     */
    public static IConstant makeConstant(BigdataValueFactory vf, URI value) {
        return new Constant(makeIV(vf, value));
    }

    /**
     * Create Constant from string and type.
     * @param vf
     * @param value
     * @return
     */
    public static IConstant makeConstant(BigdataValueFactory vf, String value, URI type) {
        return new Constant(makeIV(vf, value, type));
    }

    /**
     * Create new variable with given name.
     * @param varName
     * @return
     */
    public static IVariable makeVariable(String varName) {
        return Var.var(varName);
    }

}
