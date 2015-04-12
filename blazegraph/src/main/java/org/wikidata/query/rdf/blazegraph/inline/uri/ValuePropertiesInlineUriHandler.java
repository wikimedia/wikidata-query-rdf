package org.wikidata.query.rdf.blazegraph.inline.uri;

import java.math.BigInteger;

import com.bigdata.rdf.internal.InlineSignedIntegerURIHandler;
import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * InlineURIHandler for value and qualifier properties. Can't just use
 * InlineUnsignedIntegerURIHandler because values can end in -value. Those we
 * represent as negative numbers.
 */
public class ValuePropertiesInlineUriHandler extends InlineURIHandler {
    private static final String SUFFIX = "-value";
    private static final int SUFFIX_LENGTH = SUFFIX.length();

    public ValuePropertiesInlineUriHandler(String namespace) {
        super(namespace);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(String localName) {
        BigInteger value;
        try {
            if (localName.endsWith("-value")) {
                localName = localName.substring(0, localName.length() - SUFFIX_LENGTH);
                value = new BigInteger(localName, 10).negate();
            } else {
                value = new BigInteger(localName, 10);
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return InlineSignedIntegerURIHandler.createInlineIV(value);
    }

    @Override
    public String getLocalNameFromDelegate(AbstractLiteralIV<BigdataLiteral, ?> delegate) {
        BigInteger value = delegate.integerValue();
        if (value.compareTo(BigInteger.ZERO) < 0) {
            return value.negate().toString() + "-value";
        }
        return value.toString();
    }
}
