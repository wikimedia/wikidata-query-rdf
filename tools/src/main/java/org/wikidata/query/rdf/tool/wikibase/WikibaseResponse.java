package org.wikidata.query.rdf.tool.wikibase;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
public abstract class WikibaseResponse {

    @Nullable  private final Object error;

    @JsonCreator
    public WikibaseResponse(
            @JsonProperty("error") Object error
    ) {
        this.error = error;
    }

    /** A representation of an error if it occured, <code>null</code> otherwise. */
    public Object getError() {
        return error;
    }
}
