package org.wikidata.query.rdf.common.uri;

/**
 * Used by Wikibase RDF export to refer to information about the export itself.
 */
public class EntityData {
    /**
     * An EntityData instance for wikidata.org.
     */
    public static Entity WIKIDATA = new Entity("wikidata.org");

    private final String namespace;

    public EntityData(String host) {
        this.namespace = "http://www." + host + "/wiki/Special:EntityData/";
    }

    public String namespace() {
        return namespace;
    }
}
