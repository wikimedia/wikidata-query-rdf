package com.bigdata.rdf.internal;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * InlineURIHandler that wraps another handler than removes any trailing forward
 * slashes from the localName before giving it to the wrapped handler.
 */
public class TrailingSlashRemovingInlineUriHandler extends InlineURIHandler {
    /**
     * The handler to which to which to delegate the normalized localNames.
     */
    private final InlineURIHandler next;

    public TrailingSlashRemovingInlineUriHandler(InlineURIHandler next) {
        super(next.getNamespace());
        this.next = next;
    }

    @Override
    public void init(Vocabulary vocab) {
        super.init(vocab);
        next.init(vocab);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createInlineIV(String localName) {
        if (localName.endsWith("/")) {
            localName = localName.substring(0, localName.length() - 1);
        }
        return next.createInlineIV(localName);
    }
}
