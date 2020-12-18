package org.wikidata.query.rdf.tool.wikibase;

import java.net.URI;

import org.wikidata.query.rdf.tool.exception.ContainedException;

public class WikibaseEntityFetchException extends ContainedException {
    private final URI entityUrl;
    private final Type errorType;

    public WikibaseEntityFetchException(URI entityUrl, Type errorType) {
        super("Cannot fetch entity at " + entityUrl + ": " + errorType.name());
        this.entityUrl = entityUrl;
        this.errorType = errorType;
    }

    public URI getEntityUrl() {
        return entityUrl;
    }

    public Type getErrorType() {
        return errorType;
    }

    public enum Type {
        ENTITY_NOT_FOUND, NO_CONTENT, EMPTY_RESPONSE, UNEXPECTED_RESPONSE, UNKNOWN
    }
}
