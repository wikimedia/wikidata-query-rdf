package org.wikidata.query.rdf.tool.exception;

public class InvalidEntityIdException extends ContainedException {
    public InvalidEntityIdException(String message, Throwable cause) {
        super(message, cause);
    }
}
