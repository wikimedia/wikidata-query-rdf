package org.wikidata.query.rdf.common.uri;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Uris wikibase uses that are relative to the wikibase instance.
 */
public class WikibaseUris {
    /**
     * A WikibaseUris instance for wikidata.org.
     */
    public static final WikibaseUris WIKIDATA = new WikibaseUris("www.wikidata.org");

    /**
     * Configuration for wikibase host.
     */
    public static final String WIKIBASE_HOST_PROPERTY = "wikibaseHost";

    /**
     * Third-party prefixes filename.
     */
    public static final String WIKIBASE_PREFIXES = "wikibasePrefixes";
    /**
     * Current URI system. This is static since each instance has only one URI
     * system.
     */
    private static WikibaseUris uriSystem;

    /**
     * Extra prefixes list.
     */
    private static Map<String, String> extraPrefixes;

    /**
     * Property types used in the ontology.
     */
    public enum PropertyType {
        /**
         * Truthy predicate.
         */
        DIRECT("wdt", "direct/"),
        /**
         * Statement->Value (wdv:xxx).
         */
        STATEMENT_VALUE("psv", "statement/value/"),
        /**
         * Statement->Normalized Value (wdv:xxx).
         */
        // STATEMENT_VALUE_NORMALIZED ("psn", "statement/value-normalized/"),
        /**
         * Statement-> Simple Value.
         */
        STATEMENT("ps", "statement/"),
        /**
         * Statement->Qualifier Value (wdv:xxx).
         */
        QUALIFIER_VALUE("pqv", "qualifier/value/"),
        /**
         * Statement->Qualifier Normalized Value (wdv:xxx).
         */
        // QUALIFIER_VALUE_NORMALIZED ("pqn", "qualifier/value-normalized/"),
        /**
         * Statement-> Simple Qualifier Value.
         */
        QUALIFIER("pq", "qualifier/"),
        /**
         * Reference->Value (wdv:xxx).
         */
        REFERENCE_VALUE("prv", "reference/value/"),
        /**
         * Reference->Normalized Value (wdv:xxx).
         */
        // REFERENCE_VALUE_NORMALIZED ("prn", "reference/value-normalized/"),
        /**
         * Reference->Simple Value.
         */
        REFERENCE("pr", "reference/"),
        /**
         * Novalue class for P123.
         */
        NOVALUE("wdno", "novalue/"),
        /**
         * Entity->Statement.
         */
        CLAIM("p", "");

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
         *
         * @return prefix
         */
        public String prefix() {
            return prefix;
        }
        /**
         * Get suffix. Protected since outside classes should not use it, they
         * should go through WikibaseUris.property().
         *
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
     * The root of the wikibase uris with https prefix -
     * https://www.wikidata.org for Wikidata.
     */
    private final String rootHttps;
    /**
     * Uri prefix wikibase uses to describe exports. The Munge process removes
     * uris with this prefix.
     */
    private final String entityData;
    /**
     * Uri prefix wikibase uses to describe exports, with https prefix. The
     * Munge process removes uris with this prefix.
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
     *
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
        for (PropertyType p : PropertyType.values()) {
            query.append("PREFIX ").append(p.prefix()).append(": <")
                    .append(prop).append(p.suffix()).append(">\n");
        }
        // Add extra prefixes
        final Map<String, String> extra = getExtraPrefixes();
        for (String pref : extra.keySet()) {
            query.append("PREFIX ").append(pref).append(": <").append(prop)
                    .append(extra.get(pref)).append(">\n");
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
     * Uri prefix wikibase uses to describe exports, with https prefix. The
     * Munge process removes uris with this prefix.
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
     *
     * @return Current URI system.
     */
    public static WikibaseUris getURISystem() {
        if (uriSystem == null) {
            if (System.getProperty(WIKIBASE_HOST_PROPERTY) != null) {
                uriSystem = new WikibaseUris(
                        System.getProperty(WIKIBASE_HOST_PROPERTY));
            } else {
                uriSystem = WIKIDATA;
            }
        }
        return uriSystem;
    }

    /**
     * Get the list of extra prefixes.
     * @return
     */
    public static Map<String, String> getExtraPrefixes() {
        if (extraPrefixes == null) {
            extraPrefixes = new HashMap<>();
            if (System.getProperty(WIKIBASE_PREFIXES) != null) {
                ForbiddenOk.readPrefixes(System.getProperty(WIKIBASE_PREFIXES), extraPrefixes);
            }
        }
        return extraPrefixes;
    }

    /**
     * Methods in this class are ignored by the forbiddenapis checks. Thus you
     * need to really really really be sure what you are putting in here is
     * right.
     */
    private static class ForbiddenOk {
        /**
         * Read space-separated prefixes list into map.
         * @param filename File where prefixes are.
         * @param prefixes Prefixes map.
         */
        private static void readPrefixes(String filename, Map<String, String> prefixes) {
            try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    prefixes.put(parts[0], parts[1]);
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Bad prefix filename: " + filename);
            } catch (IOException e) {
                // not much to do here, just continue
            }
        }
    }
}
