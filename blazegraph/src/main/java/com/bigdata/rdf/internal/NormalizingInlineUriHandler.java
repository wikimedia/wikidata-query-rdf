package com.bigdata.rdf.internal;

import java.util.Arrays;
import java.util.List;

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
     * Prefixes that should be recognized as valid prefixes for this uri but are
     * not its canonical form.
     */
    private final List<String> normalizedPrefixes;

    /**
     * Build the handler.
     *
     * @param next the handler to which to send all normalized localNames
     * @param normalizedPrefixes prefixes that should be recognized as valid
     *            prefixes for this uri but are not its canonical form.
     */
    public NormalizingInlineUriHandler(InlineURIHandler next, String... normalizedPrefixes) {
        this(next, Arrays.asList(normalizedPrefixes));
    }

    public NormalizingInlineUriHandler(InlineURIHandler next, List<String> normalizedPrefixes) {
        super(next.getNamespace());
        this.next = next;
        this.normalizedPrefixes = normalizedPrefixes;
    }

    @Override
    public void init(Vocabulary vocab) {
        super.init(vocab);
        next.init(vocab);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected URIExtensionIV createInlineIV(URI uri) {
        if (namespaceIV == null) {
            // Can't do anything without a namespace.
            return null;
        }
        for (String prefix : normalizedPrefixes) {
            if (uri.stringValue().startsWith(prefix)) {
                AbstractLiteralIV localNameIv = next.createInlineIV(uri.stringValue().substring(prefix.length()));
                if (localNameIv == null) {
                    return null;
                }
                return new URIExtensionIV(localNameIv, namespaceIV);
            }
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
