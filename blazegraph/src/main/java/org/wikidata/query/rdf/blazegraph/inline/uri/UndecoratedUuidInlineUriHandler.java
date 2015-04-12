package org.wikidata.query.rdf.blazegraph.inline.uri;

import java.util.Locale;
import java.util.UUID;

import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * InlineURIHandler for UUID type URIs with not decoration - like no dashes and
 * always lowercase. Like values and references.
 */
public class UndecoratedUuidInlineUriHandler extends InlineURIHandler {
    public UndecoratedUuidInlineUriHandler(String namespace) {
        super(namespace);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(String localName) {
        localName = localName.replace("-", "");
        // localName should look like this:
        // 91212dc3fcc8b65607d27f92b36e5761
        if (localName.length() != 32) {
            return null;
        }
        try {
            long first = parseUnsigned(localName, 0, 16);
            long second = parseUnsigned(localName, 16, 32);
            return new UUIDLiteralIV(new UUID(first, second));
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    @Override
    public String getLocalNameFromDelegate(AbstractLiteralIV<BigdataLiteral, ?> delegate) {
        return delegate.stringValue().replace("-", "").toLowerCase(Locale.ROOT);
    }

    /**
     * Parse an unsigned long into a java long.
     */
    private static long parseUnsigned(String s, int from, int to) {
        long value = 0;
        for (int pos = from; pos < to; pos++) {
            int digit = Character.digit(s.charAt(pos), 16);
            if (digit == -1) {
                throw new NumberFormatException(s);
            }
            value = value * 16 + digit;
        }
        return value;
    }
}
