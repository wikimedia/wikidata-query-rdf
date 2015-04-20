package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.tool.StatementHelper.statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.hamcrest.Matcher;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Builds expanded statements in the style of Wikibase's dump flavored export or
 * post-munged. Because this is able to build both styles this amounts to the
 * best way to test the munger on expanded statements. Its therefore super
 * important that it faithfully reflects what we expect of the munger.
 */
public class ExpandedStatementBuilder {
    private final List<ExtraInfo> extraInfo = new ArrayList<>();
    private final Random random;
    private final WikibaseUris uris;
    private final String entity;
    private final String property;
    private final Object value;

    private boolean includeBasicEntity = true;
    /**
     * Version set if includeBasicEntity is true.
     */
    private Value version;
    /**
     * Date modified set if includeBasicEntity is true.
     */
    private Value dateModified;
    private ExpandedValueInfo expandedValue;

    private String statementUri;
    private String rank = Ontology.NORMAL_RANK;
    private boolean bestRank = false;

    /**
     * Null until its built and then can't be changed.
     */
    private List<Statement> statements;

    /*
     * These are built as part of the build step and used later when
     * transforming from wikibase style to munged style.
     */
    private Statement entityDataAboutDecl;
    private Statement entityDataVersionDecl;
    private Statement entityDataDateModifiedDecl;
    private Statement statementTypeDecl;

    public ExpandedStatementBuilder(Random random, WikibaseUris uris, String entity, String property, Object value) {
        this.random = random;
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
        extraInfo.add(new ReferenceInfo(property, value));
        return this;
    }

    /**
     * Add a qualifier to this statement.
     */
    public ExpandedStatementBuilder qualifier(String property, Object value) {
        extraInfo.add(new QualifierInfo(property, value));
        return this;
    }

    /**
     * Add an expanded value statement to this statement.
     */
    public ExpandedStatementBuilder expandedValue(String property, Object value) {
        if (expandedValue == null) {
            expandedValue = new ExpandedValueInfo(this.property, null);
            extraInfo.add(expandedValue);
        }
        expandedValue.entries.add(new ExpandedValueInfoEntry(property, value));
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
        Collections.shuffle(wikibaseStyle, random);
        return wikibaseStyle;
    }

    /**
     * Get the results in munged style.
     */
    public List<Statement> mungedStyle() {
        List<Statement> st = built();
        if (includeBasicEntity) {
            st.remove(entityDataAboutDecl);
            st.remove(entityDataVersionDecl);
            st.remove(entityDataDateModifiedDecl);
            statement(st, uris.entity() + entity, SchemaDotOrg.VERSION, version);
            statement(st, uris.entity() + entity, SchemaDotOrg.DATE_MODIFIED, dateModified);
        }
        st.remove(statementTypeDecl);
        for (ExtraInfo e : extraInfo) {
            e.munge(st);
        }
        return st;
    }

    public List<Matcher<? super Statement>> mungedStyleMatchers() {
        List<Matcher<? super Statement>> matchers = new ArrayList<>();
        for (Statement s : mungedStyle()) {
            matchers.add(equalTo(s));
        }
        return matchers;
    }

    public Matcher<Iterable<? extends Statement>> mungedStyleMatcher() {
        return containsInAnyOrder(mungedStyleMatchers());
    }

    private void checkCanChange() {
        if (statements != null) {
            throw new IllegalStateException("Result already built");
        }
    }

    private List<Statement> built() {
        if (statements == null) {
            build();
        }
        return new ArrayList<>(statements);
    }

    private void build() {
        statements = new ArrayList<>();
        buildBasicEntityIfNeeded();
        buildStatement();
        buildReferenceIfNeeded();
    }

    private void buildBasicEntityIfNeeded() {
        if (!includeBasicEntity) {
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

    private void buildStatement() {
        if (statementUri == null) {
            statementUri = uris.statement() + entity + "-" + randomId();
        }

        statement(statements, uris.entity() + entity, uris.entity() + property, statementUri);
        statementTypeDecl = statement(statements, statementUri, RDF.TYPE, Ontology.STATEMENT);
        statement(statements, statementUri, uris.value() + property, value);
        statement(statements, statementUri, Ontology.RANK, rank);
        if (bestRank) {
            statement(statements, statementUri, Ontology.RANK, Ontology.BEST_RANK);
        }
    }

    private void buildReferenceIfNeeded() {
        for (ExtraInfo e : extraInfo) {
            e.build();
        }
    }

    private String randomId() {
        return RandomizedTest.randomAsciiOfLength(10);
    }

    private abstract class ExtraInfo {
        protected final String property;
        protected final Object value;

        public ExtraInfo(String property, Object value) {
            this.property = property;
            this.value = value;
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

    private abstract class AbstractComplexExtraInfo extends ExtraInfo {
        protected String uri;
        private Statement typeDecl;

        public AbstractComplexExtraInfo(String property, Object value) {
            super(property, value);
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

        protected abstract String namespace();

        protected abstract String declarationPredicate();

        protected abstract String type();
    }

    private class ReferenceInfo extends AbstractComplexExtraInfo {
        public ReferenceInfo(String property, Object value) {
            super(property, value);
        }

        @Override
        public void build() {
            super.build();
            statement(statements, uri, uris.value() + property, value);
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

    private class QualifierInfo extends ExtraInfo {
        public QualifierInfo(String property, Object value) {
            super(property, value);
        }

        @Override
        public void build() {
            statement(statements, statementUri, uris.qualifier() + property, value);
        }

        @Override
        public void munge(List<Statement> statements) {
            // Intentionally a noop
        }
    }

    private class ExpandedValueInfo extends AbstractComplexExtraInfo {
        private List<ExpandedValueInfoEntry> entries = new ArrayList<>();

        public ExpandedValueInfo(String property, Object value) {
            super(property, value);
        }

        @Override
        public void build() {
            super.build();
            for (ExpandedValueInfoEntry e : entries) {
                statement(statements, uri, e.predicate, e.object);
            }
        }

        @Override
        protected String namespace() {
            return uris.value();
        }

        @Override
        protected String declarationPredicate() {
            return uris.value() + property + "-value";
        }

        @Override
        protected String type() {
            return Ontology.VALUE;
        }
    }

    private static class ExpandedValueInfoEntry {
        private final String predicate;
        private final Object object;

        public ExpandedValueInfoEntry(String predicate, Object object) {
            this.predicate = predicate;
            this.object = object;
        }
    }
}
