package org.wikidata.query.rdf.common.uri;

/**
 * Uris wikibase uses that are relative to the wikibase instance.
 */
public class WikibaseUris {
    /**
     * A WikibaseUris instance for wikidata.org.
     */
    public static final WikibaseUris WIKIDATA = new WikibaseUris("www.wikidata.org");
    /**
     * A WikibaseUris instance for test.wikidata.org.
     */
    public static final WikibaseUris TEST_WIKIDATA = new WikibaseUris("test.wikidata.org");

    /**
     * The root of the wikibase uris - http://www.wikidata.org for Wikidata.
     */
    private final String root;
    /**
     * Uri prefix wikibase uses to describe exports. The Munge process removes
     * uris with this prefix.
     */
    private final String entityData;
    /**
     * Uri prefix wikibase uses for entities. The canonical place for the entity
     * itself.
     */
    private final String entity;
    /**
     * Uri prefix wikibase uses for truthy claims.
     */
    private final String truthy;
    /**
     * Uri prefix wikibase uses for statements. They are usually of the form
     * statement:%entityId%-%a uuid%.
     */
    private final String statement;
    /**
     * Uri prefix wikibase uses for values. They are usually of the form
     * value:%a 160 bit hash of the contents%.
     */
    private final String value;
    /**
     * Uris prefix wikibase uses for references. They are usually of the form
     * reference:%a 160 bit hash of the contents%.
     */
    private final String reference;
    /**
     * Uri prefix wikibase uses for qualifiers. They are of the form
     * qualifier:%entity id of the property%(-value)?.
     */
    private final String qualifier;

    /**
     * Build for a specific wikibase host. See the WIKIDATA constant for how you
     * can use this.
     */
    public WikibaseUris(String host) {
        root = "http://" + host;
        entityData = root + "/wiki/Special:EntityData/";
        entity = root + "/entity/";
        truthy = entity + "assert/";
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
        query.append("PREFIX t: <").append(truthy).append(">\n");
        query.append("PREFIX s: <").append(statement).append(">\n");
        query.append("PREFIX v: <").append(value).append(">\n");
        query.append("PREFIX ref: <").append(reference).append(">\n");
        query.append("PREFIX q: <").append(qualifier).append(">\n");
        return query;
    }

    /**
     * The root of the wikibase uris - http://www.wikidata.org for Wikidata.
     */
    public String root() {
        return root;
    }

    /**
     * Uri prefix wikibase uses to describe exports. The Munge process removes
     * uris with this prefix.
     */
    public String entityData() {
        return entityData;
    }

    /**
     * Uri prefix wikibase uses for entities. The canonical place for the entity
     * itself.
     */
    public String entity() {
        return entity;
    }

    /**
     * Uri prefix wikibase uses for statements. They are usually of the form
     * statement:%entityId%-%a uuid%.
     */
    public String truthy() {
        return truthy;
    }

    /**
     * Prefix wikibase uses for statements.
     */
    public String statement() {
        return statement;
    }

    /**
     * Uri prefix wikibase uses for values. They are usually of the form
     * value:%a 160 bit hash of the contents%.
     */
    public String value() {
        return value;
    }

    /**
     * Uris prefix wikibase uses for references. They are usually of the form
     * reference:%a 160 bit hash of the contents%.
     */
    public String reference() {
        return reference;
    }

    /**
     * Uri prefix wikibase uses for qualifiers. They are of the form
     * qualifier:%entity id of the property%(-value)?.
     */
    public String qualifier() {
        return qualifier;
    }
}
