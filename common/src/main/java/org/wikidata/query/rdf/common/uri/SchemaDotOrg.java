package org.wikidata.query.rdf.common.uri;

/**
 * Used to specify links and things.
 */
public class SchemaDotOrg {
    public static final String NAMESPACE = "http://schema.org/";
    /**
     * Wikibase uses the MediaWiki revision as the version.
     */
    public static final String VERSION = NAMESPACE + "version";
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
}
