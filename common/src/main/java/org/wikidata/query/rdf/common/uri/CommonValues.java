package org.wikidata.query.rdf.common.uri;

/**
 * URIs that aren't really part of the wikibase model but are common values in
 * Wikidata.
 */
public final class CommonValues {
    /**
     * Virtual International Authority Files.
     */
    public static final String VIAF = "https://viaf.org/viaf/";
    /**
     * Most viaf links are declared insecure so we normalize them to their https
     * variants. We normalize to https rather than from it because we like https
     * better.
     */
    public static final String VIAF_HTTP = "http://viaf.org/viaf/";

    private CommonValues() {
        // Utility uncallable constructor
    }
}
