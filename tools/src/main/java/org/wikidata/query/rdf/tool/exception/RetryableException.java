package org.wikidata.query.rdf.tool.exception;

/**
 * The operation failed but should be retried.
 */
public class RetryableException extends Exception {
    private static final long serialVersionUID = 1384427696241404325L;

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(String message) {
        super(message);
    }
}
