package org.wikidata.query.rdf.tool.exception;

/**
 * The operation failed and the whole application is likely busted and should be
 * stopped.
 */
public class FatalException extends RuntimeException {
    private static final long serialVersionUID = 1384427696241404325L;

    public FatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalException(String message) {
        super(message);
    }
}
