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
     * Property types used in the ontology.
     */
    public enum PropertyType {
        /**
         * Truthy predicate.
         */
        DIRECT          ("wdt", "direct/"),
        /**
         * Statement->Value (wdv:).
         */
        STATEMENT_VALUE ("psv", "statement/value/"),
        /**
         * Statement-> Simple Value.
         */
        STATEMENT       ("ps",  "statement/"),
        /**
         * Statement->Qualifier Value (wdv:).
         */
        QUALIFIER_VALUE ("pqv", "qualifier/value/"),
        /**
         * Statement-> Simple Qualifier Value.
         */
        QUALIFIER       ("pq",  "qualifier/"),
        /**
         * Reference->Value (wdv:).
         */
        REFERENCE_VALUE ("prv", "reference/value/"),
        /**
         * Reference->Simple Value.
         */
        REFERENCE       ("pr",  "reference/"),
        /**
         * Novalue class for P123.
         */
        NOVALUE         ("wdno", "novalue/"),
        /**
         * Entity->Statement.
         */
        CLAIM           ("p",    "");

        /**
         * Short prefix for the type.
         */
        private final String prefix;
        /**
         * Url suffix after /prop/ for the type.
         */
        private final String suffix;

        PropertyType(String p, String s) {
            this.prefix = p;
            this.suffix = s;
        }
        /**
         * Get prefix.
         * @return prefix
         */
        public String prefix() {
            return prefix;
        }
        /**
         * Get suffix.
         * Protected since outside classes should not use it, they should go through
         * WikibaseUris.property().
         * @return suffix
         */
        protected String suffix() {
            return suffix;
        }
    };

    /**
     * The root of the wikibase uris - http://www.wikidata.org for Wikidata.
     */
    private final String root;
    /**
     * The root of the wikibase uris with https prefix - https://www.wikidata.org for Wikidata.
     */
    private final String rootHttps;
    /**
     * Uri prefix wikibase uses to describe exports. The Munge process removes
     * uris with this prefix.
     */
    private final String entityData;
    /**
     * Uri prefix wikibase uses to describe exports, with https prefix.
     * The Munge process removes uris with this prefix.
     */
    private final String entityDataHttps;
    /**
     * Uri prefix wikibase uses for entities. The canonical place for the entity
     * itself.
     */
    private final String entity;
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
     * Uri property prefix, used for properties.
     * @see PropertyType
     */
    private final String prop;

    /**
     * Build for a specific wikibase host. See the WIKIDATA constant for how you
     * can use this.
     */
    public WikibaseUris(String host) {
        root = "http://" + host;
        rootHttps = "https://" + host;
        entityData = root + "/wiki/Special:EntityData/";
        entityDataHttps = rootHttps + "/wiki/Special:EntityData/";
        entity = root + "/entity/";
        statement = entity + "statement/";
        value = root + "/value/";
        reference = root + "/reference/";
        prop = root + "/prop/";
    }

    /**
     * Add the prefixes for all related uris.
     */
    public StringBuilder prefixes(StringBuilder query) {
        query.append("PREFIX wdata: <").append(entityData).append(">\n");
        query.append("PREFIX wd: <").append(entity).append(">\n");
        query.append("PREFIX wds: <").append(statement).append(">\n");
        query.append("PREFIX wdv: <").append(value).append(">\n");
        query.append("PREFIX wdref: <").append(reference).append(">\n");
        for (PropertyType p: PropertyType.values()) {
            query.append("PREFIX ").append(p.prefix()).append(": <").append(prop).append(p.suffix()).append(">\n");
        }
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
     * Uri prefix wikibase uses to describe exports, with https prefix.
     * The Munge process removes uris with this prefix.
     */
    public String entityDataHttps() {
        return entityDataHttps;
    }

    /**
     * Uri prefix wikibase uses for entities. The canonical place for the entity
     * itself.
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
     * Uri prefix wikibase uses for values. They are usually of the form
     * value:%a 128 bit hash of the contents%.
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
    public String property(PropertyType p) {
        return prop + p.suffix();
    }

    /**
     * Return current URI system.
     * @return
     */
    public static WikibaseUris getURISystem() {
        // FIXME: make it possible to configure URI system here
        return WIKIDATA;
    }
}
