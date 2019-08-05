package org.wikidata.query.rdf.common.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * URI scheme for Wikibase RDF representation.
 * See the documentation for Wikidata implementation here:
 * https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format
 */
public class WikibaseUris {
    /**
     * Prefix for entity URI.
     */
    public static final String ENTITY_PREFIX = "wd";
    /**
     * Prefix for entity data URI.
     */
    public static final String ENTITY_DATA_PREFIX = "wdata";

    /**
     * Initial letters of supported Wikibase items.
     * L is not included because L IDs can not be inlined due
     * to conflict between Lexemes, Forms and Senses.
     */
    private static final ImmutableList<String> WIKIBASE_INITIALS = ImmutableList.of("Q", "P");

    /**
     * Property types used in the ontology.
     */
    public enum PropertyType {
        /**
         * Truthy predicate.
         */
        DIRECT("wdt", "direct/"),
        /**
         * Truthy normalized predicate.
         */
        DIRECT_NORMALIZED("wdtn", "direct-normalized/"),
        /**
         * Statement - Value (wdv:xxx).
         */
        STATEMENT_VALUE("psv", "statement/value/"),
        /**
         * Statement - Normalized Value (wdv:xxx).
         */
        STATEMENT_VALUE_NORMALIZED("psn", "statement/value-normalized/"),
        /**
         * Statement -  Simple Value.
         */
        STATEMENT("ps", "statement/"),
        /**
         * Statement - Qualifier Value (wdv:xxx).
         */
        QUALIFIER_VALUE("pqv", "qualifier/value/"),
        /**
         * Statement - Qualifier Normalized Value (wdv:xxx).
         */
        QUALIFIER_VALUE_NORMALIZED("pqn", "qualifier/value-normalized/"),
        /**
         * Statement - Simple Qualifier Value.
         */
        QUALIFIER("pq", "qualifier/"),
        /**
         * Reference - Value (wdv:xxx).
         */
        REFERENCE_VALUE("prv", "reference/value/"),
        /**
         * Reference - Normalized Value (wdv:xxx).
         */
        REFERENCE_VALUE_NORMALIZED("prn", "reference/value-normalized/"),
        /**
         * Reference - Simple Value.
         */
        REFERENCE("pr", "reference/"),
        /**
         * Novalue class for P123.
         */
        NOVALUE("wdno", "novalue/"),
        /**
         * Entity - Statement.
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
         */
        protected String suffix() {
            return suffix;
        }

        /**
         * Get the list of property suffixes as list of strings.
         */
        public static List<String> suffixes() {
            return suffixes(values());
        }

        /**
         * Get the list of property suffixes as list of strings.
         */
        public static List<String> suffixes(PropertyType[] values) {
            return Stream.of(values).map(v -> v.suffix)
                    .collect(Collectors.toList());
        }

        /**
         * Types list as it was in V001 dictionary.
         * Used for BC. V002 has the same set, but V003 is different.
         */
        public static PropertyType[] V001() {
            return new PropertyType[] {
                DIRECT,
                STATEMENT_VALUE,
                STATEMENT,
                QUALIFIER_VALUE,
                QUALIFIER,
                REFERENCE_VALUE,
                REFERENCE,
                NOVALUE,
                CLAIM
            };
        }
    };

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

    public WikibaseUris(URI conceptUrl) {
        root = conceptUrl.toString().replaceAll("/+$", "");
        entityData = root + "/wiki/Special:EntityData/";
        entityDataHttps = otherScheme(conceptUrl) + "/wiki/Special:EntityData/";
        entity = root + "/entity/";
        statement = entity + "statement/";
        value = root + "/value/";
        reference = root + "/reference/";
        prop = root + "/prop/";
    }

    /**
     * Return the representation of URI in different scheme.
     * https <-> http
     * @return URL string in other scheme
     */
    private String otherScheme(URI uri) {
        if (uri.getScheme().equals("http")) {
            return uri.toString().replace("http:", "https:").replaceAll("/+$", "");
        } else {
            return uri.toString().replace("https:", "http:").replaceAll("/+$", "");
        }
    }

    /**
     * Add the prefixes for all related uris.
     */
    @SuppressFBWarnings(value = "CBX_CUSTOM_BUILT_XML", justification = "false positive - not actually XML")
    public StringBuilder prefixes(StringBuilder query) {
        entityPrefixes().forEach((k, v) -> {
            query.append("PREFIX ").append(k).append(": <").append(v).append(">\n");
        });
        query.append("PREFIX wds: <").append(statement).append(">\n");
        query.append("PREFIX wdv: <").append(value).append(">\n");
        query.append("PREFIX wdref: <").append(reference).append(">\n");
        for (PropertyType p : PropertyType.values()) {
            query.append("PREFIX ").append(p.prefix()).append(": <")
                    .append(prop).append(p.suffix()).append(">\n");
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
    protected String entity() {
        return entity;
    }

    /**
     * Convert entity ID to full URI.
     * @return Full entity URI
     */
    public String entityIdToURI(String entityId) {
        return entity + entityId;
    }

    /**
     * Convert entity URI to entity ID.
     * @return entity ID, or original string if it wasn't entity URI.
     */
    public String entityURItoId(String uri) {
        if (uri.startsWith(entity)) {
            return uri.substring(entity.length());
        }
        return uri;
    }

    /**
     * Check whether the argument is an entity URI.
     */
    public boolean isEntityURI(String uri) {
        return uri.startsWith(entity);
    }

    /**
     * Get the list of all possible entity prefixes.
     * For now it's just one but could be more.
     */
    public Collection<String> entityURIs() {
        return Collections.singletonList(entity);
    }

    /**
     * Map of RDF prefixes for entities to full URIs.
     * E.g. wd: http://www.wikidata.org/entity/
     */
    public Map<String, String> entityPrefixes() {
        return ImmutableMap.of(ENTITY_PREFIX, entity, ENTITY_DATA_PREFIX, entityData());
    }

    /**
     * Get the list of letters that can start entities.
     */
    public Collection<String> entityInitials() {
        return WIKIBASE_INITIALS;
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
     * Uri prefix wikibase uses for property types.
     */
    public String property(PropertyType p) {
        return property(p.suffix());
    }

    /**
     * Uri prefix wikibase uses for property types, from short suffix.
     */
    public String property(String suffix) {
        return prop + suffix;
    }

    /**
     * Build for a specific wikibase host. See the WIKIDATA constant for how you
     * can use this.
     */
    public static WikibaseUris forHost(String host) {
        try {
            if (host == null) {
                return UriSchemeFactory.WIKIDATA;
            }
            return new WikibaseUris(new URI("http://" + host));
        } catch (URISyntaxException e) {
            throw new RuntimeException("Bad URI host: " + host, e);
        }
    }

}
