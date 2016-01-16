package org.wikidata.query.rdf.test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.google.common.collect.ImmutableList;

/**
 * Constructs statements for testing.
 */
public final class StatementHelper {
    /**
     * Statement build helper.
     */
    public static Statement statement(String s, String p, Object o) {
        Value oValue;
        if (o instanceof String) {
            oValue = uri(o.toString());
        } else if (o instanceof Value) {
            oValue = (Value) o;
        } else if (o instanceof Integer) {
            oValue = new IntegerLiteralImpl(BigInteger.valueOf((int) o));
        } else if (o instanceof Long) {
            oValue = new IntegerLiteralImpl(BigInteger.valueOf((long) o));
        } else {
            throw new IllegalArgumentException("Illegal object:  " + o);
        }
        if (p instanceof String && p.startsWith("P")) {
            p = WikibaseUris.getURISystem().property(PropertyType.CLAIM) + p;
        }
        return new StatementImpl(uri(s), uri(p), oValue);
    }

    /**
     * Statement build helper.
     */
    public static Statement statement(List<Statement> statements, String s, String p, Object o) {
        Statement st = statement(s, p, o);
        statements.add(st);
        return st;
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
                    statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(language)), //
                    statement(link, SchemaDotOrg.ABOUT, entityId), //
                    statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE));

        }
        return ImmutableList.of(//
                statement(link, RDF.TYPE, SchemaDotOrg.ARTICLE), //
                statement(link, SchemaDotOrg.ABOUT, entityId), //
                statement(link, SchemaDotOrg.IN_LANGUAGE, new LiteralImpl(language)));
    }

    /**
     * Convert a string into a URI for testing.
     */
    public static URI uri(String r) {
        if (r.startsWith("Q") || r.startsWith("P")) {
            return new URIImpl(WikibaseUris.getURISystem().entity() + r);
        }
        return new URIImpl(r);
    }

    /**
     * Construct statements about a basic entity.
     */
    public static List<Statement> basicEntity(WikibaseUris uris, String id) {
        Literal version = new LiteralImpl("a revision number I promise");
        List<Statement> statements = new ArrayList<>();
        String entityDataUri = uris.entityData() + id;
        // EntityData is all munged onto Entity
        statement(statements, entityDataUri, SchemaDotOrg.ABOUT, id);
        statement(statements, entityDataUri, SchemaDotOrg.VERSION, version);
        statement(statements, entityDataUri, SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("a date I promise"));
        return statements;
    }

    /**
     * Construct a random date literal.
     */
    public static LiteralImpl randomDate() {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);
        formatter.format("%04d-%02d-%02d", randomIntBetween(1, 9999), randomIntBetween(1, 12), randomIntBetween(1, 28));
        formatter.close();
        return new LiteralImpl(sb.toString(), XMLSchema.DATE);
    }

    /**
     * Construct a bunch of random statements about the given subject.
     */
    public static List<Statement> randomStatementsAbout(String s, int count) {
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String p = "P" + randomInt();
            LiteralImpl o = randomDate();
            statements.add(statement(s, p, o));
        }
        return statements;
    }

    private StatementHelper() {
        // Uncallable entity constructor
    }
}
