package org.wikidata.query.rdf.common.uri;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static PropertyType[] v001() {
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
}
