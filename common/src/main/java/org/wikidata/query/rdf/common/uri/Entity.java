package org.wikidata.query.rdf.common.uri;

/**
 * Used to prefix entities in Wikibase.
 */
public class Entity {
    /**
     * An Entity instance for wikidata.org.
     */
    public static Entity WIKIDATA = new Entity("wikidata.org");

    private final String namespace;

    public Entity(String host) {
        this.namespace = "http://www." + host + "/entity/";
    }

    public String namespace() {
        return namespace;
    }
}
