package org.wikidata.query.rdf.common.uri;

/**
 * Used by Wikibase RDF export to refer to information about the export itself.
 */
public class EntityData {
    /**
     * An EntityData instance for wikidata.org.
     */
    public static EntityData WIKIDATA = new EntityData("www.wikidata.org");

    private final String namespace;

    public EntityData(String host) {
        this.namespace = "http://" + host + "/wiki/Special:EntityData/";
    }

    public String namespace() {
        return namespace;
    }
}
