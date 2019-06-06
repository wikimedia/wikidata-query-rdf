package org.wikidata.query.rdf.common;

/**
 * Naming conventions for logging.
 */
public final class LoggingNames {

    private LoggingNames() {
        // utility class, should not be constructed
    }

    /** A query sent by Blazegraph to MediaWiki. */
    public static final String MW_API_REQUEST = "mw-api-request";
    /** A query sent by Blazegraph to remote service. */
    public static final String REMOTE_REQUEST = "remote-request";

}
