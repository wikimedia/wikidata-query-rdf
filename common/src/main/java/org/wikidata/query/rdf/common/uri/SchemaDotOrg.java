package org.wikidata.query.rdf.common.uri;

/**
 * Used to specify links and things.
 */
public final class SchemaDotOrg {
    /**
     * Common prefix for all schema.org uris.
     */
    public static final String NAMESPACE = "http://schema.org/";
    /**
     * Wikibase uses the MediaWiki revision as the version.
     */
    public static final String VERSION = NAMESPACE + "version";
    /**
     * Wikibase uses the MediaWiki revision as the version.
     */
    public static final String SOFTWARE_VERSION = NAMESPACE + "softwareVersion";
    /**
     * Wikibase adds this to EntityData with the date of the revision of the entity.
     */
    public static final String DATE_MODIFIED = NAMESPACE + "dateModified";
    /**
     * Wikibase uses this to link the EntityData information to the Entity information.
     */
    public static final String ABOUT = NAMESPACE + "about";
    /**
     * Wikibase spits out sitelinks as <code>&lt;url&gt; rdf:type schema:article .</code>.
     */
    public static final String ARTICLE = NAMESPACE + "Article";
    /**
     * Wikibase dumps the label in this, skos:prefLabel and rdfs:label. We only
     * keep rdfs:label.
     */
    public static final String NAME = NAMESPACE + "name";
    /**
     * Wikibase marks the sitelink's language with this predicate.
     */
    public static final String IN_LANGUAGE = NAMESPACE + "inLanguage";
    /**
     * Wikibase adds the descriptions as schema:description.
     */
    public static final String DESCRIPTION = NAMESPACE + "description";

    /**
     * Adds the schema: prefix to the query.
     */
    public static StringBuilder prefix(StringBuilder query) {
        return query.append("PREFIX schema: <").append(NAMESPACE).append(">\n");
    }

    /**
     * Utility class uncallable constructor.
     */
    private SchemaDotOrg() {
        // Utility class.
    }
}
