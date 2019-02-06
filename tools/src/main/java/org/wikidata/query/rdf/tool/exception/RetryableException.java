package org.wikidata.query.rdf.tool.exception;

/**
 * The operation failed but should be retried.
 */
public class RetryableException extends Exception {
    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(String message) {
        super(message);
    }
}
