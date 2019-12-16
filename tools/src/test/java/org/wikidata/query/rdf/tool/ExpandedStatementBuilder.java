package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hamcrest.Matcher;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.test.Randomizer;

/**
 * Builds expanded statements in the style of Wikibase's dump flavored export or
 * post-munged. Because this is able to build both styles this amounts to the
 * best way to test the munger on expanded statements. Its therefore super
 * important that it faithfully reflects what we expect of the munger.
 */
public class ExpandedStatementBuilder {
    /**
     * Extra parts of the statement like qualifiers and references.
     */
    private final List<ExtraInfo> extraInfo = new ArrayList<>();
    /**
     * Randomizer used to power and random decisions. Passed around so randomized
     * testing can always seed properly.
     */
    private final Randomizer randomizer;
    /**
     * Uris for the wikibase instance.
     */
    private final UrisScheme uris;
    /**
     * Entity id that is the subject of the statement.
     */
    private final String entity;
    /**
     * Property id that is the predicate in the statement. Its also used in the
     * expanded portion.
     */
    private final String property;
    /**
     * The value of the statement. Its also used in the expanded version.
     */
    private final Object value;

    /**
     * Should the results include the basic entity information for the subject
     * so its valid? Should be set to false if combining the results of multiple
     * builders and generally should be left true otherwise.
     */
    private boolean includeBasicEntityForSubject = true;
    /**
     * Version set if includeBasicEntity is true.
     */
    private Value version;
    /**
     * Date modified set if includeBasicEntity is true.
     */
    private Value dateModified;
    /**
     * Uri for the statement. Generated if not set.
     */
    private String statementUri;
    /**
     * Rank of the statement.
     */
    private String rank = Ontology.NORMAL_RANK;
    /**
     * Is the statement also best rank? Defaults to false.
     */
    private boolean bestRank;
    /**
     * Null until its built and then can't be changed.
     */
    private List<Statement> statements;

    /*
     * These are built as part of the build step and used later when
     * transforming from wikibase style to munged style.
     */
    /**
     * Part of the wikibase dump that is removed by the munger.
     */
    private Statement entityDataAboutDecl;
    /**
     * Part of the wikibase dump that is removed by the munger.
     */
    private Statement entityDataVersionDecl;
    /**
     * Part of the wikibase dump that is removed by the munger.
     */
    private Statement entityDataDateModifiedDecl;
    /**
     * Part of the wikibase dump that is removed by the munger.
     */
    private Statement statementTypeDecl;

    /**
     * Setup required configuration. Everything else can be configured.
     */
    public ExpandedStatementBuilder(Randomizer randomizer, UrisScheme uris, String entity, String property, Object value) {
        this.randomizer = randomizer;
        this.uris = uris;
        this.entity = entity;
        this.property = property;
        this.value = value;
    }

    /**
     * Get the entity of this expanded statement.
     */
    public String entity() {
        return entity;
    }

    /**
     * Get the value of this expanded statement.
     */
    public Object value() {
        return value;
    }

    /**
     * Should this statement be of best rank?
     */
    public ExpandedStatementBuilder bestRank(boolean bestRank) {
        checkCanChange();
        this.bestRank = bestRank;
        return this;
    }

    /**
     * Add a reference to this statement.
     */
    public ExpandedStatementBuilder reference(String property, Object value) {
        checkCanChange();
        extraInfo.add(new ReferenceInfo(property, value));
        return this;
    }

    /**
     * Add a qualifier to this statement.
     */
    public ExpandedStatementBuilder qualifier(String property, Object value) {
        checkCanChange();
        extraInfo.add(new QualifierInfo(property, value));
        return this;
    }

    /**
     * Get the results in wikibase style.
     */
    public List<Statement> wikibaseStyle() {
        return built();
    }

    /**
     * Get the results in wikibase style but shuffled.
     */
    public List<Statement> wikibaseStyleShuffled() {
        List<Statement> wikibaseStyle = wikibaseStyle();
        Collections.shuffle(wikibaseStyle, randomizer.getRandom());
        return wikibaseStyle;
    }

    /**
     * Get the results in munged style.
     */
    public List<Statement> mungedStyle() {
        List<Statement> st = built();
        if (includeBasicEntityForSubject) {
            st.remove(entityDataAboutDecl);
            st.remove(entityDataVersionDecl);
            st.remove(entityDataDateModifiedDecl);
            String entityURI = uris.entityIdToURI(entity);
            statement(st, entityURI, SchemaDotOrg.VERSION, version);
            statement(st, entityURI, SchemaDotOrg.DATE_MODIFIED, dateModified);
        }
        st.remove(statementTypeDecl);
        for (ExtraInfo e : extraInfo) {
            e.munge(st);
        }
        return st;
    }

    /**
     * Build a list of matchers that matches what the statements should look
     * like after they've been munged.
     */
    public List<Matcher<? super Statement>> mungedStyleMatchers() {
        List<Matcher<? super Statement>> matchers = new ArrayList<>();
        for (Statement s : mungedStyle()) {
            matchers.add(equalTo(s));
        }
        return matchers;
    }

    /**
     * Build a matcher that matches what the statements should look like after
     * they've been munged.
     */
    public Matcher<Iterable<? extends Statement>> mungedStyleMatcher() {
        return containsInAnyOrder(mungedStyleMatchers());
    }

    /**
     * Can this builder change?
     */
    private void checkCanChange() {
        if (statements != null) {
            throw new IllegalStateException("Result already built");
        }
    }

    /**
     * Get a mutable copy of the list of triples for this expanded statement,
     * building the triple list if its not already been built.
     */
    private List<Statement> built() {
        if (statements == null) {
            build();
        }
        return new ArrayList<>(statements);
    }

    /**
     * Build the statements for this expanded statement.
     */
    private void build() {
        statements = new ArrayList<>();
        buildBasicEntityIfNeeded();
        buildStatement();
        buildExtraInfo();
    }

    /**
     * Build the basic entity for the subject if includeBasicEntityForSubject.
     */
    private void buildBasicEntityIfNeeded() {
        if (!includeBasicEntityForSubject) {
            return;
        }
        if (version == null) {
            version = new LiteralImpl("a revision number I promise");
        }
        if (dateModified == null) {
            dateModified = new LiteralImpl("a date I promise");
        }
        String entityDataUri = uris.entityData() + entity;
        entityDataAboutDecl = statement(statements, entityDataUri, SchemaDotOrg.ABOUT, entity);
        entityDataVersionDecl = statement(statements, entityDataUri, SchemaDotOrg.VERSION, version);
        entityDataDateModifiedDecl = statement(statements, entityDataUri, SchemaDotOrg.DATE_MODIFIED, dateModified);
    }

    /**
     * Build triples for the expanded statement.
     */
    private void buildStatement() {
        if (statementUri == null) {
            statementUri = uris.statement() + entity + "-" + randomId();
        }

        statement(statements, uris.entityIdToURI(entity), uris.entityIdToURI(property), statementUri);
        statementTypeDecl = statement(statements, statementUri, RDF.TYPE, Ontology.STATEMENT);
        statement(statements, statementUri, uris.value() + property, value);
        statement(statements, statementUri, Ontology.RANK, rank);
        if (bestRank) {
            statement(statements, statementUri, Ontology.RANK, Ontology.BEST_RANK);
        }
    }

    /**
     * Build the extra triples on the statement like references and qualifiers.
     */
    private void buildExtraInfo() {
        for (ExtraInfo e : extraInfo) {
            e.build();
        }
    }

    /**
     * Randomly make a statement or reference identifier.
     */
    private String randomId() {
        return randomizer.randomAsciiOfLength(10);
    }

    /**
     * Extra bits of the expanded statement.
     */
    private abstract class ExtraInfo {
        /**
         * The property for this extra info.
         */
        private final String property;
        /**
         * The value of this extra info.
         */
        private final Object value;

        ExtraInfo(String property, Object value) {
            this.property = property;
            this.value = value;
        }

        /**
         * The property for this extra info.
         */
        public String property() {
            return property;
        }

        /**
         * The value of this expanded info.
         */
        public Object value() {
            return value;
        }

        /**
         * Build the statements representing this extra.
         */
        public abstract void build();

        /**
         * Perform whatever munging we expect the munger to perform.
         */
        public abstract void munge(List<Statement> statements);
    }

    /**
     * Useful base for building references and qualifiers.
     */
    private abstract class AbstractComplexExtraInfo extends ExtraInfo {
        /**
         * Uri for the expanded part.
         */
        private String uri;
        /**
         * Type declaration for the expanded part.
         */
        private Statement typeDecl;

        AbstractComplexExtraInfo(String property, Object value) {
            super(property, value);
        }

        /**
         * Uri for the expanded part.
         */
        public String uri() {
            return uri;
        }

        @Override
        public void build() {
            if (uri == null) {
                uri = namespace() + entity + "-" + randomId();
            }
            statement(statements, statementUri, declarationPredicate(), uri);
            typeDecl = statement(statements, uri, RDF.TYPE, type());
        }

        @Override
        public void munge(List<Statement> statements) {
            statements.remove(typeDecl);
        }

        /**
         * In what namespace is the value?
         */
        protected abstract String namespace();

        /**
         * What is the predicate for triple?
         */
        protected abstract String declarationPredicate();

        /**
         * What is the type of the resulting expanded entity (reference,
         * qualifier, etc)?
         */
        protected abstract String type();
    }

    /**
     * Builds a reference.
     */
    private class ReferenceInfo extends AbstractComplexExtraInfo {
        ReferenceInfo(String property, Object value) {
            super(property, value);
        }

        @Override
        public void build() {
            super.build();
            statement(statements, uri(), uris.value() + property(), value());
        }

        @Override
        protected String namespace() {
            return uris.reference();
        }

        @Override
        protected String declarationPredicate() {
            return Provenance.WAS_DERIVED_FROM;
        }

        @Override
        protected String type() {
            return Ontology.REFERENCE;
        }
    }

    /**
     * Build qualifiers.
     */
    private class QualifierInfo extends ExtraInfo {
        QualifierInfo(String property, Object value) {
            super(property, value);
        }

        @Override
        public void build() {
            statement(statements, statementUri, uris.property(PropertyType.QUALIFIER) + property(), value());
        }

        @Override
        public void munge(List<Statement> statements) {
            // Intentionally a noop
        }
    }

    /**
     * Part of the expanded value. An example is the declaration that a geo
     * coordinant is in a certain globe or a time has a certain precision.
     */
    private static class ExpandedValueInfoEntry {
        /**
         * Predicate of the triple.
         */
        private final String predicate;
        /**
         * Object of the triple.
         */
        private final Object object;

        ExpandedValueInfoEntry(String predicate, Object object) {
            this.predicate = predicate;
            this.object = object;
        }
    }
}
