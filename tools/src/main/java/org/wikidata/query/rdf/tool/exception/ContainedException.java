package org.wikidata.query.rdf.tool.exception;

/**
 * The operation failed and likely won't succeed with those parameters unless
 * some action is taken but the rest of the operation should proceed.
 */
public class ContainedException extends RuntimeException {
    public ContainedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ContainedException(String message) {
        super(message);
    }
}
