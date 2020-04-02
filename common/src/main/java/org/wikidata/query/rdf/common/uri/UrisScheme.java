package org.wikidata.query.rdf.common.uri;

import java.util.Collection;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

@Immutable
public interface UrisScheme {
    /**
     * Add the prefixes for all related uris.
     */
    StringBuilder prefixes(StringBuilder query);

    /**
     * The root of the wikibase uris - http://www.wikidata.org for Wikidata.
     */
    String root();

    /**
     * Uri prefix wikibase uses to describe exports. The Munge process removes
     * uris with this prefix.
     */
    String entityData();

    /**
     * Uri prefix wikibase uses to describe exports, with https prefix. The
     * Munge process removes uris with this prefix.
     */
    String entityDataHttps();

    /**
     * Convert entity ID to full URI.
     * @return Full entity URI
     */
    String entityIdToURI(String entityId);

    /**
     * Convert entity URI to entity ID.
     * @return entity ID, or original string if it wasn't entity URI.
     */
    String entityURItoId(String uri);

    /**
     * Check whether the argument is an entity URI.
     */
    boolean isEntityURI(String uri);

    /**
     * Get the list of all possible entity prefixes.
     */
    Collection<String> entityURIs();

    /**
     * Map of RDF prefixes for entities to full URIs.
     * E.g. wd: http://www.wikidata.org/entity/
     */
    Map<String, String> entityPrefixes();

    /**
     * Get the list of letters that can start entities.
     */
    Collection<String> entityInitials();

    /**
     * Prefix wikibase uses for statements.
     */
    String statement();

    /**
     * Uri prefix wikibase uses for values. They are usually of the form
     * value:%a 128 bit hash of the contents%.
     */
    String value();

    /**
     * Uris prefix wikibase uses for references. They are usually of the form
     * reference:%a 160 bit hash of the contents%.
     */
    String reference();

    /**
     * Uri prefix wikibase uses for property types.
     */
    String property(PropertyType p);

    /**
     * Uri prefix wikibase uses for property types, from short suffix.
     */
    String property(String suffix);

    /**
     * Prefix used for skolems of blank nodes.
     * c.f. RDF 1.1§3.5
     */
    String wellKnownBNodeIRIPrefix();

    boolean supportsUri(String uri);

    boolean supportsInitial(String entityId);
}
