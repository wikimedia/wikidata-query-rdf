package org.wikidata.query.rdf.tool.exception;

/**
 * Exception caused by bad parameters in HTTP request.
 */
public class BadParameterException extends ContainedException {
    public BadParameterException(String message) {
        super(message);
    }
}
