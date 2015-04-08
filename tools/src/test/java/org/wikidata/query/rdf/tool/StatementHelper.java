package org.wikidata.query.rdf.tool;

import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.google.common.collect.ImmutableList;

/**
 * Constructs statements for testing.
 */
public class StatementHelper {
    /**
     * Statement constructor taking just URIs as strings.
     */
    public static Statement statement(String s, String p, String o) {
        return statement(s, p, uri(o));
    }

    /**
     * Statement constructor with a value. Use this one for all values.
     */
    public static Statement statement(String s, String p, Value o) {
        return new StatementImpl(uri(s), uri(p), o);
    }

    /**
     * Statement constructor taking just URIs as strings and appending the
     * statement to a list.
     */
    public static Statement statement(List<Statement> statements, String s, String p, String o) {
        return statement(statements, s, p, uri(o));
    }

    /**
     * Statement constructor with a value appending the statement to a list. Use
     * this one for all values.
     */
    public static Statement statement(List<Statement> statements, String s, String p, Value o) {
        Statement statement = statement(s, p, o);
        statements.add(statement);
        return statement;
    }

    /**
     * Build the statements describing a sitelink.
     *
     * @param entityId entity being linked
     * @param link address of the link
     * @param language language the link is in
     * @return statements describing the sitelink
     */
    public static ImmutableList<Statement> siteLink(String entityId, String link, String language) {
        return siteLink(entityId, link, language, false);
    }

    /**
     * Build the statements describing a sitelink.
     *
     * @param entityId entity being linked
     * @param link address of the link
     * @param language language the link is in
     * @param outOfOrder should the link be out of order compared to how
     *            wikidata dumps it?
     * @return statements describing the sitelink
     */

    public static ImmutableList<Statement> siteLink(String entityId, String link, String language, boolean outOfOrder) {
        if (outOfOrder) {
            return ImmutableList.of(//
                    statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(language)),//
                    statement(link, SchemaDotOrg.ABOUT, entityId),//
                    statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE));

        }
        return ImmutableList.of(//
                statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE),//
                statement(link, SchemaDotOrg.ABOUT, entityId),//
                statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(language)));
    }

    /**
     * Convert a string into a URI for testing.
     */
    public static URI uri(String r) {
        if (r.startsWith("Q") || r.startsWith("P")) {
            return new URIImpl(WikibaseUris.WIKIDATA.entity() + r);
        }
        return new URIImpl(r);
    }
}
