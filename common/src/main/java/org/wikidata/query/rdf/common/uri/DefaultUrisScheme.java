package org.wikidata.query.rdf.common.uri;

import static com.google.common.collect.ImmutableList.copyOf;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * URI scheme for Wikibase RDF representation.
 * See the documentation for Wikidata implementation here:
 * https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format
 */
@Immutable
public class DefaultUrisScheme implements UrisScheme {

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

    private final String entityPrefix;
    private final String entityDataPrefix;
    private final List<String> initials;
    private final String wellKnownBNodeIRIPrefix;

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification = "wikibaseInitials needs to be a List, order is important here")
    public DefaultUrisScheme(URI conceptUrl, String entityPrefix, String entityDataPrefix, List<String> wikibaseInitials) {
        root = conceptUrl.toString().replaceAll("/+$", "");
        entityData = root + "/wiki/Special:EntityData/";
        entityDataHttps = httpsScheme(conceptUrl) + "/wiki/Special:EntityData/";
        entity = root + "/entity/";
        statement = entity + "statement/";
        wellKnownBNodeIRIPrefix = generateWellKnownURI(conceptUrl, "genid/");
        value = root + "/value/";
        reference = root + "/reference/";
        prop = root + "/prop/";
        this.entityPrefix = entityPrefix;
        this.entityDataPrefix = entityDataPrefix;
        initials = copyOf(wikibaseInitials);
    }

    /**
     * Generate a well-known URI as defined in RFC5785.
     */
    private static String generateWellKnownURI(URI conceptUrl, String what) {
        return conceptUrl.getScheme() + "://" + conceptUrl.getHost() + (conceptUrl.getPort() > 0 ? ":" + conceptUrl.getPort() : "") + "/.well-known/" + what;
    }

    /**
     * Return the representation of URI in HTTPS.
     * http -> https
     * @return URL string in other scheme
     */
    private String httpsScheme(URI uri) {
        String uriString;
        if (uri.getScheme().equals("http")) {
            uriString = uri.toString().replace("http:", "https:");
        } else {
            uriString = uri.toString();
        }
        return uriString.replaceAll("/+$", "");
    }

    @Override
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

    @Override
    public String root() {
        return root;
    }

    @Override
    public String entityData() {
        return entityData;
    }

    @Override
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

    @Override
    public String entityIdToURI(String entityId) {
        return entity + entityId;
    }

    @Override
    public String entityURItoId(String uri) {
        if (uri.startsWith(entity)) {
            return uri.substring(entity.length());
        }
        return uri;
    }

    @Override
    public boolean isEntityURI(String uri) {
        return uri.startsWith(entity);
    }

    @Override
    public Collection<String> entityURIs() {
        return Collections.singletonList(entity);
    }

    @Override
    public Map<String, String> entityPrefixes() {
        return ImmutableMap.of(entityPrefix, entity, entityDataPrefix, entityData());
    }

    @Override
    public List<String> inlinableEntityInitials() {
        return initials;
    }

    @Override
    public String statement() {
        return statement;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public String reference() {
        return reference;
    }

    @Override
    public String property(PropertyType p) {
        return property(p.suffix());
    }

    @Override
    public String property(String suffix) {
        return prop + suffix;
    }

    @Override
    public String wellKnownBNodeIRIPrefix() {
        return wellKnownBNodeIRIPrefix;
    }

    @Override
    public boolean supportsUri(String uri) {
        return uri.startsWith(root());
    }
}
