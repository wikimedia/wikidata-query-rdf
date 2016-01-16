package com.bigdata.rdf.internal;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * InlineURIHandler that wraps another handler, normalizing multiple uri
 * prefixes into one.
 */
public class NormalizingInlineUriHandler extends InlineURIHandler {
    /**
     * The wrapped handler to which everything is delegated once normalized.
     */
    private final InlineURIHandler next;

    /**
     * Build the handler.
     *
     * @param next the handler to which to send all normalized localNames
     * @param normalizedPrefix prefix that should be recognized as valid
     *            prefix for this uri but is not its canonical form.
     */
    public NormalizingInlineUriHandler(InlineURIHandler next, String normalizedPrefix) {
        super(normalizedPrefix);
        this.next = next;
    }

    @Override
    public void init(Vocabulary vocab) {
 // Skip init() since we have no vocab entry for our namespace
//     super.init(vocab);
        next.init(vocab);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public URIExtensionIV createInlineIV(URI uri) {
        String prefix = getNamespace();
        if (uri.stringValue().startsWith(prefix)) {
            AbstractLiteralIV localNameIv = next.createInlineIV(uri.stringValue().substring(prefix.length()));
            if (localNameIv == null) {
                return null;
            }
            return new URIExtensionIV(localNameIv, next.namespaceIV);
        }
        return next.createInlineIV(uri);
    }

    @Override
    public String getLocalNameFromDelegate(AbstractLiteralIV<BigdataLiteral, ?> delegate) {
        return next.getLocalNameFromDelegate(delegate);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(String localName) {
        return next.createInlineIV(localName);
    }
}
