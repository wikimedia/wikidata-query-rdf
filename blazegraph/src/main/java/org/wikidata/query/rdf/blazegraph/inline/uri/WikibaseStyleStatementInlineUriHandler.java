package org.wikidata.query.rdf.blazegraph.inline.uri;

import java.math.BigInteger;
import java.util.Locale;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Attempts to inline statements as a BigInteger. The sign and the top bits
 * contain the entity id and the uuid portion lives in the bottom 128 bits.
 *
 * Since storing unsigned integer takes up such an incredible amount of space
 * this is unused. It really really really really bloats the index. This class
 * is more here are as a word of warning and historical artifact than anything
 * useful.
 *
 * While it might be tempting to just encode the whole statement as a uuid and
 * throw away the leading entity id that is a huge mistake because it causes the
 * statements to get scattered along the index which causes mighty right
 * amplification during loads and updates and probably doesn't help query
 * performance either.
 * @deprecated
 */
public class WikibaseStyleStatementInlineUriHandler extends InlineURIHandler {
    private static final Logger log = Logger.getLogger(WikibaseStyleStatementInlineUriHandler.class);

    public WikibaseStyleStatementInlineUriHandler(String namespace) {
        super(namespace);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(String localName) {
        switch (localName.charAt(0)) {
        case 'q':
        case 'Q':
            return inlineIvFrom(1, localName);
        case 'p':
        case 'P':
            return inlineIvFrom(-1, localName);
        default:
            try {
                return new UUIDLiteralIV(UUID.fromString(localName));
            } catch (IllegalArgumentException e) {
                log.debug("Invalid uuid:  " + localName, e);
                return null;
            }
        }
    }

    @Override
    public String getLocalNameFromDelegate(AbstractLiteralIV<BigdataLiteral, ?> delegate) {
        switch (delegate.getDTE()) {
        case UUID:
            // UUID style statements just decode.
            return delegate.stringValue().toUpperCase(Locale.ROOT);
        case XSDInteger:
            // Otherwise we've got to decode the BigInteger style:
            BigInteger i = delegate.integerValue();
            long least = i.longValue();
            i = i.shiftRight(Long.SIZE);
            long most = i.longValue();
            i = i.shiftRight(Long.SIZE);
            long entity = i.longValue();
            StringBuilder b = new StringBuilder();
            if (entity < 0) {
                entity = -entity;
                b.append('P');
            } else {
                b.append('Q');
            }
            b.append(entity).append('-').append(new UUID(most, least).toString().toUpperCase(Locale.ROOT));
            return b.toString();
        default:
            // How in the world did we get here?
            return super.getLocalNameFromDelegate(delegate);
        }
    }

    /**
     * Build the delegate holding the uri from the localname with the prefix
     * already resolved into signum.
     */
    @SuppressWarnings("rawtypes")
    private static AbstractLiteralIV inlineIvFrom(int signum, String localName) {
        int firstDash = localName.indexOf('-');
        long entity = Long.valueOf(localName.substring(1, firstDash), 10);
        try {
            UUID u = UUID.fromString(localName.substring(firstDash + 1));
            BigInteger i = BigInteger.valueOf(signum * entity);
            i = i.shiftLeft(Long.SIZE).or(unsigned(u.getMostSignificantBits()));
            i = i.shiftLeft(Long.SIZE).or(unsigned(u.getLeastSignificantBits()));
            return new XSDIntegerIV(i);
        } catch (IllegalArgumentException e) {
            Logger.getLogger(WikibaseStyleStatementInlineUriHandler.class).warn("tmp", e);
            return null;
        }
    }

    /**
     * Convert a long into an unsigned BigInteger holding it.
     */
    private static BigInteger unsigned(long l) {
        BigInteger i = BigInteger.valueOf(l & 0x7fffffffffffffffL);
        if (l < 0) {
            i = i.setBit(Long.SIZE - 1);
        }
        return i;
    }
}
