package org.wikidata.query.rdf.common.uri;

/**
 * Uris wikibase uses that are relative to the wikibase instance.
 */
public class WikibaseUris {
    /**
     * An Entity instance for wikidata.org.
     */
    public static WikibaseUris WIKIDATA = new WikibaseUris("www.wikidata.org");
    public static WikibaseUris TEST_WIKIDATA = new WikibaseUris("test.wikidata.org");

    private final String entityData;
    private final String entity;
    private final String statement;
    private final String value;
    private final String reference;
    private final String qualifier;

    public WikibaseUris(String host) {
        String root = "http://" + host;
        entityData = root + "/wiki/Special:EntityData/";
        entity = root + "/entity/";
        statement = entity + "statement/";
        value = entity + "value/";
        reference = entity + "reference/";
        qualifier = entity + "qualifier/";
    }

    /**
     * Add the prefixes for all related uris.
     */
    public StringBuilder prefixes(StringBuilder query) {
        query.append("PREFIX data: <").append(entityData).append(">\n");
        query.append("PREFIX entity: <").append(entity).append(">\n");
        query.append("PREFIX s: <").append(statement).append(">\n");
        query.append("PREFIX v: <").append(value).append(">\n");
        query.append("PREFIX ref: <").append(reference).append(">\n");
        query.append("PREFIX q: <").append(qualifier).append(">\n");
        return query;
    }

    /**
     * Prefix wikibase uses for dump information about entities.
     */
    public String entityData() {
        return entityData;
    }

    /**
     * Prefix wikibase uses for entities.
     */
    public String entity() {
        return entity;
    }

    /**
     * Prefix wikibase uses for statements.
     */
    public String statement() {
        return statement;
    }

    /**
     * Prefix wikibase uses for values in statements.
     */
    public String value() {
        return value;
    }

    /**
     * Prefix wikibase uses for references.
     */
    public String reference() {
        return reference;
    }

    /**
     * Prefix wikibase uses for the predicates on qualifiers.
     */
    public String qualifier() {
        return qualifier;
    }
}
