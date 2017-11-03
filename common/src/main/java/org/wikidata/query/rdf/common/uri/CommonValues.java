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
    /**
     * Prefix for geonames URI: http://sws.geonames.org/$1/.
     * See: https://www.wikidata.org/wiki/Property:P1566
     */
    public static final String GEONAMES = "http://sws.geonames.org/";
    /**
     * PubChem compound ID.
     * See: https://www.wikidata.org/wiki/Property:P662
     */
    public static final String PUBCHEM = "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID";

    /**
     * ChemSpider compound ID.
     * See: https://www.wikidata.org/wiki/Property:P661
     */
    public static final String CHEMSPIDER = "http://rdf.chemspider.com/";

    private CommonValues() {
        // Utility uncallable constructor
    }
}
