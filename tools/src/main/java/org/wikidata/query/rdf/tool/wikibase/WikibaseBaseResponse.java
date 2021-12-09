package org.wikidata.query.rdf.tool.wikibase;

import javax.annotation.Nullable;

/**
 * Base class for all responses from Wikibase.
 *
 * All Wikibase responses can return errors in the same format. Subclasses
 * should implement the more specific fields.
 *
 * Note that error message is parsed as a very generic {@link Object}. This
 * Object will be a String in all known cases, but might be a more complex
 * structure if the response contains some complex structure.
 */
public abstract class WikibaseBaseResponse implements WikibaseResponse {

    @Nullable
    private final Object error;

    protected WikibaseBaseResponse(@Nullable Object error) {
        this.error = error;
    }

    /** A representation of an error if it occured, <code>null</code> otherwise. */
    @Nullable
    public Object getError() {
        return error;
    }
}
