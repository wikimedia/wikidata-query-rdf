package org.wikidata.query.rdf.test;

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
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
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
        if (p.startsWith("P")) {
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
        if (r.startsWith("Q") || r.startsWith("P") || r.startsWith("L")) {
            return new URIImpl(WikibaseUris.getURISystem().entity() + r);
        }
        return new URIImpl(r);
    }

    public static List<Statement> basicEntity(WikibaseUris uris, String id) {
        return basicEntity(uris, id, "a revision number I promise");
    }
    /**
     * Construct statements about a basic entity.
     */
    public static List<Statement> basicEntity(WikibaseUris uris, String id, String versionString) {
        Literal version = new LiteralImpl(versionString);
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
     * @param randomizer
     */
    public static LiteralImpl randomDate(Randomizer randomizer) {
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);
        formatter.format(
                "%04d-%02d-%02d",
                randomizer.randomIntBetween(1, 9999), randomizer.randomIntBetween(1, 12), randomizer.randomIntBetween(1, 28));
        formatter.close();
        return new LiteralImpl(sb.toString(), XMLSchema.DATE);
    }

    /**
     * Construct a bunch of random statements about the given subject.
     */
    public static List<Statement> randomStatementsAbout(Randomizer randomizer, String s, int count) {
        List<Statement> statements = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String p = "P" + randomizer.randomInt();
            LiteralImpl o = randomDate(randomizer);
            statements.add(statement(s, p, o));
        }
        return statements;
    }

    private StatementHelper() {
        // Uncallable entity constructor
    }

    /**
     * Helper class for building more complex statements.
     * Allows building complex statements for certain entity.
     * See https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format
     * for the docs about how complex statements are built. Note that since
     * it is a test facility, the data is only skeletal.
     */
    public static class StatementBuilder {
        /**
         * Current list of statements.
         */
        private List<Statement> statements;
        /**
         * Entity ID for statement collection.
         */
        private final String entityId;
        /**
         * Statement we're working with now.
         */
        private String currentStatement;
        /**
         * Property ID for tatement we're working with now.
         */
        private String currentProperty;
        /**
         * URI system.
         */
        private final WikibaseUris uris = WikibaseUris.getURISystem();

        public StatementBuilder(String entityId) {
            this.entityId = entityId;
            statements = new ArrayList<>();
        }

        public StatementBuilder withStatement(String propertyId, String statementUri) {
            statement(statements, entityId, propertyId, statementUri);
            currentStatement = statementUri;
            currentProperty = propertyId;
            return this;
        }

        public StatementBuilder withStatementValue(String valueUri) {
            statement(statements, currentStatement, uris.property(PropertyType.STATEMENT_VALUE) + currentProperty, valueUri);
            return this;
        }

        public StatementBuilder withStatementValueNormalized(String valueUri) {
            statement(statements, currentStatement, uris.property(PropertyType.STATEMENT_VALUE_NORMALIZED) + currentProperty, valueUri);
            return this;
        }

        public StatementBuilder withTimeValue(String valueUri, String timeValue) {
            statement(statements, valueUri, Ontology.Time.VALUE, new LiteralImpl(timeValue));
            return this;
        }

        public StatementBuilder withQuanitityValue(String valueUri, String value) {
            statement(statements, valueUri, Ontology.Quantity.AMOUNT, new LiteralImpl(value));
            return this;
        }

        public StatementBuilder withQuanitityValueNormalized(String valueUri,
                String value, String normalizedUri,
                String normalizedValue) {
            statement(statements, valueUri, Ontology.Quantity.AMOUNT, new LiteralImpl(value));
            statement(statements, normalizedUri, Ontology.Quantity.AMOUNT, new LiteralImpl(normalizedValue));
            statement(statements, valueUri, Ontology.Quantity.NORMALIZED, normalizedUri);
            statement(statements, normalizedUri, Ontology.Quantity.NORMALIZED, normalizedUri);
            return this;
        }

        public StatementBuilder withTimeCalendarValue(String valueUri, String timeValue, String calendar) {
            withTimeValue(valueUri, timeValue);
            statement(statements, valueUri, Ontology.Time.CALENDAR_MODEL, new LiteralImpl(calendar));
            return this;
        }

        public StatementBuilder withReference(String referenceUri, String propertyId, String value) {
            statement(statements, currentStatement, Provenance.WAS_DERIVED_FROM, referenceUri);
            statement(statements, referenceUri, uris.property(PropertyType.REFERENCE) + propertyId, value);
            return this;
        }

        public StatementBuilder withReferenceValue(String referenceUri, String propertyId, String value) {
            statement(statements, currentStatement, Provenance.WAS_DERIVED_FROM, referenceUri);
            statement(statements, referenceUri, uris.property(PropertyType.REFERENCE_VALUE) + propertyId, value);
            return this;
        }

        public StatementBuilder withEntityData(String version, String lastModified) {
            statement(statements, uris.entityData() + entityId, SchemaDotOrg.VERSION, new LiteralImpl(version));
            statement(statements, uris.entityData() + entityId, SchemaDotOrg.DATE_MODIFIED, new LiteralImpl(lastModified));
            return this;
        }

        /**
         * Add statement to entity itself.
         */
        public StatementBuilder withPredicateObject(String predicate, Object object) {
            statement(statements, entityId, predicate, object);
            return this;
        }

        public List<Statement> build() {
            return statements;
        }
    }

}
