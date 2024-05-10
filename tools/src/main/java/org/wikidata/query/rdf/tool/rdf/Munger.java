package org.wikidata.query.rdf.tool.rdf;

import static java.util.function.UnaryOperator.identity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.WikibasePoint.CoordinateOrder;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Ontology.Lexeme;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.exception.ContainedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Munges RDF from Wikibase into a more queryable format. Note that this is
 * tightly coupled with Wikibase's export format.
 */
public final class Munger {
    private static final Logger LOG = LoggerFactory.getLogger(Munger.class);
    /**
     * Wikibase uris we're working with.
     */
    private final UrisScheme uris;
    /**
     * Null if not in limit label languages mode and a set of allowed languages
     * if in it.
     */
    private final Set<String> limitLabelLanguages;
    /**
     * Null if not in single label mode or a fallback chain of language if in
     * it. The fallback chain is reversed so a higher index in the list means a
     * "better" language.
     */
    private final List<String> singleLabelModeLanguages;
    /**
     * True if we should remove site links or false if we shouldn't.
     */
    private final boolean removeSiteLinks;

    /**
     * True if we want to keep types for Statement and Item.
     */
    private final boolean keepTypes;
    private final ValueFactory valueFactory;
    private final UnaryOperator<Statement> statementFilter;

    /**
     * Predicates that are UriScheme dependent.
     */
    private final NamespaceStatementPredicates predicates;


    /**
     * Format version we're dealing with.
     */
    private String dumpFormatVersion;

    /**
     * Types that we will remove from data.
     */
    private static final Set<String> SKIPPED_TYPES = ImmutableSet
            .of(Ontology.ITEM, Lexeme.LEXEME, Lexeme.FORM, Lexeme.SENSE);

    /**
     * Map of format handlers.
     */
    private final Map<String, FormatHandler> formatHandlers;

    private Munger(UrisScheme uris, Set<String> limitLabelLanguages, List<String> singleLabelModeLanguages,
                   boolean removeSiteLinks, boolean keepTypes, Map<String, FormatHandler> formatHandlers,
                   ValueFactory valueFactory, UnaryOperator<Statement> statementFilter) {
        this.uris = uris;
        this.limitLabelLanguages = limitLabelLanguages;
        this.singleLabelModeLanguages = singleLabelModeLanguages;
        this.removeSiteLinks = removeSiteLinks;
        this.keepTypes = keepTypes;
        this.formatHandlers = formatHandlers;
        this.valueFactory = valueFactory;
        this.statementFilter = statementFilter;
        this.predicates = new NamespaceStatementPredicates(uris);
    }

    /**
     * Set format version.
     */
    public void setFormatVersion(String version) {
        this.dumpFormatVersion = version;
    }

    @VisibleForTesting
    String getDumpFormatVersion() {
        return this.dumpFormatVersion;
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     * @return the revision data from statements, as Change. At least revision ID and timestamp will
     * be accurate.
     */
    public long munge(String entityId,
                      Collection<Statement> statements) {
        MungeOperation op = new MungeOperation(entityId, statements);
        op.munge();
        return op.getRevisionId();
    }

    public static Builder builder(UrisScheme uris) {
        return new Builder(uris);
    }

    public static final class Builder {

        private UrisScheme uris;
        private Collection<String> limitLabelLanguages;
        private List<String> singleLabelModeLanguages;
        private boolean removeSiteLinks;
        private boolean keepTypes;
        private Map<String, FormatHandler> formatHandlers = new HashMap<>();
        private ValueFactory valueFactory;
        private boolean convertBNodesToSkolemIRIs;

        Builder(UrisScheme uris) {
            this.uris = uris;

            // 0.0.1 has format lat-long, 0.0.2 has format long-lat
            // Depending on which default we have now, we want to switch one of them
            if (WikibasePoint.DEFAULT_ORDER == CoordinateOrder.LAT_LONG) {
                addFormatHandler("0.0.2", new PointCoordinateSwitcher());
            } else {
                addFormatHandler("0.0.1", new PointCoordinateSwitcher());
            }
        }

        /**
         * Build a Munger that removes site links.
         */
        public Builder removeSiteLinks() {
            removeSiteLinks = true;
            return this;
        }

        /**
         * Build a Munger that only imports labels in some languages.
         */
        public Builder limitLabelLanguages(String... languages) {
            return limitLabelLanguages(Arrays.asList(languages));
        }

        /**
         * Build a Munger that only imports labels in some languages.
         */
        public Builder limitLabelLanguages(Collection<String> languages) {
            limitLabelLanguages = languages;
            return this;
        }

        public Builder valueFactory(ValueFactory valueFactory) {
            this.valueFactory = valueFactory;
            return this;
        }

        public Builder convertBNodesToSkolemIRIs(boolean convertBNodesToSkolemIRIs) {
            this.convertBNodesToSkolemIRIs = convertBNodesToSkolemIRIs;
            return this;
        }

        /**
         * Build a munger that will load only a single label per entity. Note that
         * if there isn't a label in one of the languages then there will be no
         * label for the entity.
         *
         * @param languages a fallback chain of languages with the first one being
         *            the most important
         */
        public Builder singleLabelMode(String... languages) {
            return singleLabelMode(Arrays.asList(languages));
        }

        /**
         * Build a munger that will load only a single label per entity. Note that
         * if there isn't a label in one of the languages then there will be no
         * label for the entity.
         *
         * @param languages a fallback chain of languages with the first one being
         *            the most important
         */
        public Builder singleLabelMode(List<String> languages) {
            singleLabelModeLanguages = languages;
            return this;
        }

        /**
         * Set the keep types parameter.
         */
        public Builder keepTypes(boolean keep) {
            keepTypes = keep;
            return this;
        }

        /**
         * Add handler for specific non-default format.
         */
        @VisibleForTesting
        Builder addFormatHandler(String version, FormatHandler handler) {
            formatHandlers.put(version, handler);
            return this;
        }

        public Munger build() {
            ValueFactory vF = this.valueFactory != null ? this.valueFactory : new ValueFactoryImpl();
            UnaryOperator<Statement> statementFilter;
            if (convertBNodesToSkolemIRIs) {
                statementFilter = new BNodeSkolemization(vF, uris.wellKnownBNodeIRIPrefix());
            } else {
                statementFilter = identity();
            }
            return new Munger(
                    uris,
                    limitLabelLanguages == null ? null : ImmutableSet.copyOf(limitLabelLanguages),
                    singleLabelModeLanguages == null ? null : ImmutableList.copyOf(singleLabelModeLanguages).reverse(),
                    removeSiteLinks,
                    keepTypes,
                    ImmutableMap.copyOf(formatHandlers),
                    vF,
                    statementFilter);
        }

    }

    /**
     * Interface to handle format transformations.
     */
    public interface FormatHandler {
        /**
         * Transform statement to current latest format.
         * @return Transformed statement or null if it needs to be deleted.
         */
        Statement handle(Statement statement);
    }

    /**
     * Holds state during a single munge operation.
     */
    private class MungeOperation {
        /**
         * The uri of the entity we're processing.
         */
        private final String entityUri;
        /**
         * The statements that we're processing.
         */
        private final Collection<Statement> statements;
        /**
         * The entity uri that we're working with.
         */
        private final Resource entityUriResource;
        /*
         * These are modified during the pass over the statements and used to
         * modify the statements during cleanup
         */
        /**
         * A list of statements that were removed from the original collection
         * in error.
         */
        private final List<Statement> restoredStatements = new ArrayList<>();
        /**
         * Sub-entities of this entity.
         */
        private final Set<String> subEntities = new HashSet<>();
        /**
         * Subjects of all sitelinks.
         */
        private final Set<String> siteLinks = new HashSet<>();
        /**
         * Valid non-site link subjects.
         */
        private final Set<String> extraValidSubjects = new HashSet<>();
        /**
         * Subjects that likely showed up in statements in error. If a later
         * statement merits the re-inclusion of the subject then its statements
         * will be removed from this multimap and added to restoredStatement.
         */
        private final ListMultimap<String, Statement> unknownSubjects = ArrayListMultimap.create();
        /**
         * Work used in single label mode to find the best label and null if not
         * in single label mode.
         */
        private final SingleLabelModeWork singleLabelModeWorkForLabel;
        /**
         * Work used in single label mode to find the best description and null
         * if not in single label mode.
         */
        private final SingleLabelModeWork singleLabelModeWorkForDescription;

        /**
         * Statements that belong to data entity.
         * We will transfer them to the item/property.
         */
        private final Set<Pair<URI, Literal>> dataStatements = new HashSet<>();

        // These are set by the entire munge operation
        /**
         * Revision id that we find while scanning the statements.
         */
        private Literal revisionId;

        /**
         * Last modified date that we find while scanning the statements.
         */
        private Literal lastModified;

        // These are setup by munge and reset for every statement
        /**
         * Format handler for current format.
         */
        private FormatHandler formatHandler;

        MungeOperation(String entityId, Collection<Statement> statements) {
            this.statements = statements;
            entityUri = uris.entityIdToURI(entityId);
            entityUriResource = valueFactory.createURI(entityUri);
            if (singleLabelModeLanguages != null) {
                singleLabelModeWorkForLabel = new SingleLabelModeWork();
                singleLabelModeWorkForDescription = new SingleLabelModeWork();
            } else {
                singleLabelModeWorkForLabel = null;
                singleLabelModeWorkForDescription = null;
            }
            setFormatVersion(dumpFormatVersion);

        }

        /**
         * Set current version of the format.
         */
        private void setFormatVersion(String version) {
            this.formatHandler = formatHandlers.get(version);
        }

        /**
         * Get revision ID for this change.
         */
        private long getRevisionId() {
            return revisionId == null ? -1 : Long.parseLong(revisionId.stringValue());
        }

        /**
         * Munge the statements.
         */
        public void munge() {
            Iterator<Statement> itr = statements.iterator();
            while (itr.hasNext()) {
                Statement statement = itr.next();
                Statement handled = filter(statement);
                if (handled == null) {
                    // drop it
                    itr.remove();
                } else if (!handled.equals(statement)) {
                    // modified
                    itr.remove();
                    statement = handled;
                    if (statement(statement)) {
                        // if we accept it in modified form, add back
                        restoredStatements.add(statement);
                    }
                } else if (!statement(statement)) {
                    itr.remove();
                }
            }

            finishSingleLabelMode();
            finishCommon();
        }

        private Statement filter(Statement statement) {
            if (formatHandler != null) {
                statement = formatHandler.handle(statement);
            }
            if (statement == null) {
                return null;
            }
            statement = statementFilter.apply(statement);
            return trimLargeObject(statement);
        }

        /**
         * Check whether object's length is more than 32k.
         * If so, create new statement that cuts object down to 32k.
         * @return New statement or the same statement if not needed.
         */
        private Statement trimLargeObject(Statement statement) {
            if (statement.getObject() instanceof Literal) {
                final Literal value = (Literal) statement.getObject();
                if (value.stringValue().length() > Short.MAX_VALUE) {
                    final Literal newValue;
                    if (value.getDatatype().equals(org.openrdf.model.vocabulary.RDF.LANGSTRING)) {
                        newValue = valueFactory.createLiteral(value.stringValue().substring(0, Short.MAX_VALUE), value.getLanguage());
                    } else {
                        newValue = valueFactory.createLiteral(value.stringValue().substring(0, Short.MAX_VALUE), value.getDatatype());
                    }
                    return valueFactory.createStatement(statement.getSubject(),
                            statement.getPredicate(), newValue);
                }
            }
            return statement;
        }

        /**
         * Process a statement.
         *
         * @return true to keep the statement, false to remove it
         */
        @SuppressWarnings("checkstyle:npathcomplexity")
        private boolean statement(Statement statement) {
            String subject = statement.getSubject().stringValue();
            if (predicates.subjectInEntityDataNS(statement)) {
                return entityDataStatement(statement);
            }
            if (predicates.subjectInStatementNS(statement)) {
                return entityStatementStatement(statement);
            }
            if (predicates.subjectInReferenceNS(statement)) {
                return entityReferenceStatement(statement);
            }
            if (predicates.subjectInValueNS(statement)) {
                return entityValueStatement(statement);
            }
            if (uris.isEntityURI(subject)) {
                return entityStatement(statement);
            }
            if (subject.startsWith(uris.property(PropertyType.CLAIM))) {
                return propertyStatement(statement);
            }
            /*
             *  Allow bnodes, they are not linked to specific entity
             *  but used to declare classes
             */
            if (predicates.subjectIsBNodeOrSkolemIRI(statement)) {
                return true;
            }
            return unknownStatement(statement);
        }

        /**
         * Process statement about property.
         * @return true to keep the statement, false to remove it
         */
        private boolean propertyStatement(Statement statement) {
            // This is wdno:P123 a owlClass, owl:complementOf _:blah - allow it
            if (statement.getSubject().stringValue().startsWith(uris.property(PropertyType.NOVALUE))) {
                return true;
            }
            // It's p:P2762 a owl:ObjectProperty, it's ok.
            return statement.getPredicate().stringValue().equals(RDF.TYPE);
        }

        /**
         * Process a statement who's subject is in the entityData prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityDataStatement(Statement statement) {
            boolean knownPredicate = false;
            // Three specific ones are recorded for later re-application
            String predicate = statement.getPredicate().stringValue();
            switch (predicate) {
                case SchemaDotOrg.VERSION:
                    knownPredicate = true;
                    revisionId = objectAsLiteral(statement);
                    break;
                case SchemaDotOrg.DATE_MODIFIED:
                    knownPredicate = true;
                    lastModified = objectAsLiteral(statement);
                    break;
                case SchemaDotOrg.SOFTWARE_VERSION:
                    setFormatVersion(objectAsLiteral(statement).stringValue());
                    break;
                default:
                    if (predicate.startsWith(Ontology.NAMESPACE)) {
                        knownPredicate = true;
                    }
                    // Noop - fall out is ok as we just remove them.
            }
            if (knownPredicate) {
                dataStatements.add(new ImmutablePair<>(
                        statement.getPredicate(), objectAsLiteral(statement)));
            }
            // All EntityData statements are removed.
            return false;
        }

        /**
         * Process a statement who's subject is in the entity prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        private boolean entityStatement(Statement statement) {
            String subject = statement.getSubject().stringValue();
            if (!subject.equals(entityUri) && !subEntities.contains(subject)) {
                /*
                 * Some flavors of rdf dump information about other entities
                 * along side the main entity. We can't handle that properly and
                 * it doesn't make a ton of sense anyway.
                 */
                return false;
            }
            switch (statement.getPredicate().stringValue()) {
                case RDF.TYPE:
                    if (keepTypes) {
                        return true;
                    }
                    /*
                     * We don't need wd:Q1 a ontology:Item because its super common
                     * and not super interesting.
                     */
                    return !SKIPPED_TYPES.contains(statement.getObject().stringValue());
                case SchemaDotOrg.NAME:
                case SKOS.PREF_LABEL:
                    // Q1 schema:name "foo" is a dupe of rdfs:label
                    // Q1 skos:prefLabel "foo" is a dupe of rdfs:label
                    return false;
                case RDFS.LABEL:
                    if (uris.entityURItoId(subject).startsWith("L")
                            || subEntities.contains(subject)) {
                        // Skip labels for Lexeme & its sub-entities, e.g. Forms and Senses
                        return false;
                    }
                    return limitLabelLanguage(statement) && singleLabelMode(singleLabelModeWorkForLabel, statement);
                case SchemaDotOrg.DESCRIPTION:
                    return limitLabelLanguage(statement) && singleLabelMode(singleLabelModeWorkForDescription, statement);
                case SKOS.ALT_LABEL:
                    return limitLabelLanguage(statement);
                case OWL.SAME_AS:
                    return true;
                case Ontolex.LEXICAL_FORM:
                case Ontolex.SENSE_PREDICATE:
                    // Links to Form and Sense
                    subEntities.add(statement.getObject().stringValue());
                    return true;
                default:
                    return entityStatementWithUnrecognizedPredicate(statement);
            }
        }

        /**
         * Process a statement about an entity who's predicate isn't explicitly
         * recognized.
         */
        private boolean entityStatementWithUnrecognizedPredicate(Statement statement) {
            if (predicates.reificationStatement(statement)) {
                registerExtraValidSubject(statement.getObject().stringValue());
            }
            // Most statements should be kept.
            return true;
        }

        /**
         * Process a statement who's subject is in the entity statement prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityStatementStatement(Statement statement) {
            String subject = statement.getSubject().stringValue();
            switch (statement.getPredicate().stringValue()) {
                case RDF.TYPE:
                    if (keepTypes) {
                        return true;
                    }
                    /*
                     * We don't need s:<uuid> a ontology:Statement because its super
                     * common and not super interesting.
                     */
                    if (statement.getObject().stringValue().equals(Ontology.STATEMENT)) {
                        return false;
                    }
                    break;
                case Provenance.WAS_DERIVED_FROM:
                    if (predicates.objectInReferenceNS(statement)) {
                        registerExtraValidSubject(statement.getObject().stringValue());
                    }
                    return true;
                default:
            }
            if (!extraValidSubjects.contains(subject)) {
                /*
                 * Remove any statements about statements that we don't yet know
                 * are actually linked to the entity. We keep a backup of them
                 * in unknownSubjects so we can restore them if they are later
                 * linked.
                 */
                if (!statement.getPredicate().stringValue().equals(Ontology.CONSTRAINT_VIOLATION)) {
                    // We just ignore constraint violations for unknown statements
                    unknownSubjects.put(subject, statement);
                }
                return false;
            }
            if (predicates.objectInValueNS(statement)) {
                registerExtraValidSubject(statement.getObject().stringValue());
            }
            return true;
        }

        /**
         * Process a statement who's subject is in the entity reference prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityReferenceStatement(Statement statement) {
            String object = statement.getObject().stringValue();
            String subject = statement.getSubject().stringValue();

            if (StatementPredicates.typeStatement(statement)) {
                if (keepTypes) {
                    return true;
                }
                /*
                 * We don't need r:<uuid> a ontology:Reference because its super
                 * common and not super interesting.
                 */
                if (StatementPredicates.referenceTypeStatement(statement)) {
                    return false;
                }
            }
            if (!extraValidSubjects.contains(subject)) {
                /*
                 * Remove any statements about references that we don't yet know
                 * are actually linked to the entity. We keep a backup of them
                 * in unknownSubjects so we can restore them if they are later
                 * linked.
                 */
                unknownSubjects.put(subject, statement);
                return false;
            }
            if (predicates.tripleRefValue(statement)) {
                registerExtraValidSubject(object);
            }
            return true;
        }

        /**
         * Process a statement who's subject is in the entity value prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityValueStatement(Statement statement) {
            String subject = statement.getSubject().stringValue();
            switch (statement.getPredicate().stringValue()) {
                case RDF.TYPE:
                    // We keep value types
                    return true;
                case Quantity.NORMALIZED:
                    /* Keep normalized values. It's a bit tricky here since
                     * normalized value may not be used yet and when we restore
                     * regular value we may miss normalized one. So we always add
                     * normalized ones to allowed list.
                     */
                    registerExtraValidSubject(statement.getObject().stringValue());
                    break;
                default:
            }
            if (!extraValidSubjects.contains(subject)) {
                /*
                 * Remove any statements about values that we don't yet know are
                 * actually linked to the entity. We keep a backup of them in
                 * unknownSubjects so we can restore them if they are later
                 * linked.
                 */
                unknownSubjects.put(subject, statement);
                return false;
            }
            return true;
        }

        /**
         * Process a statement who's subject isn't in the entityData or entity
         * prefixes.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean unknownStatement(Statement statement) {
            String predicate = statement.getPredicate().stringValue();
            String subject = statement.getSubject().stringValue();
            // This is <https://it.wikipedia.org/> wikibase:wikiGroup "wikipedia" .
            if (predicate.equals(Ontology.WIKIGROUP)) {
                return true;
            }

            if (siteLinks.contains(subject)) {
                return !removeSiteLinks;
            }
            if (extraValidSubjects.contains(subject)) {
                return true;
            }

            if (predicate.equals(RDF.TYPE) && statement.getObject().stringValue().equals(SchemaDotOrg.ARTICLE)) {
                siteLinks.add(subject);
                /*
                 * Site links may have crept into unknown subjects if they
                 * appeared in a funky order. Restore them or clear them as
                 * appropriate.
                 */
                if (removeSiteLinks) {
                    unknownSubjects.removeAll(subject);
                    return false;
                } else {
                    restoredStatements.addAll(unknownSubjects.removeAll(subject));
                    return true;
                }
            }
            /*
             * Record and remove all truly unknown statements so we can either
             * complain about them at the end of the process of restore them if
             * they turn out to be part of a site link or something.
             */
            unknownSubjects.put(subject, statement);
            return false;
        }

        /**
         * Process a label type statement for the limitlLabelLanguage feature.
         * This is a noop if the feature is disabled but if it is enabled then
         * the label will be removed (returns false) if the label's language
         * isn't in limitLabelLanguages.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean limitLabelLanguage(Statement statement) {
            if (limitLabelLanguages == null) {
                return true;
            }
            Literal object = objectAsLiteral(statement);
            String language = object.getLanguage();
            return language != null && limitLabelLanguages.contains(language);
        }

        /**
         * Handle a label or statement in single label mode or just keep the
         * label if we're not in single label mode.
         */
        private boolean singleLabelMode(SingleLabelModeWork work, Statement statement) {
            return work == null || work.statement(statement);
        }

        /**
         * Perform all munge completion tasks that are required no matter the
         * configuration. Its important that finishCommon be the last finish
         * method called because it is the one that restores restoredStatments
         * into the original statements collection.
         */
        private void finishCommon() {
            if (!unknownSubjects.isEmpty()) {
                /*
                 * If we have any valid statements, we ignore the garbage.
                 * Otherwise, something wrong is going on and we reject the
                 * update.
                 */
                if (statements.isEmpty() && restoredStatements.isEmpty()) {
                    throw new BadSubjectException(unknownSubjects.keySet(), uris);
                } else {
                    LOG.info(
                            "Unrecognized subjects: {} while processing {}.  Expected only sitelinks and subjects starting with {} and {}",
                            unknownSubjects.keySet(), entityUri, uris.entityData(), uris.entityURIs());
                    // Log statements we're about to drop, unless there are too many
                    unknownSubjects.entries().stream()
                            .limit(20)
                            .forEach(
                                    e -> LOG.info(
                                            "Unrecognized statement: s:{} p:{} o:{}",
                                            e.getValue().getSubject(), e.getValue().getPredicate(), e.getValue().getObject()));
                    if (unknownSubjects.size() > 20) {
                        LOG.info("More than 20 unrecognized statements, further statements not logged.");
                    }
                }
            }
            if (revisionId == null) {
                throw new ContainedException("Didn't get a revision id for " + statements);
            }
            if (lastModified == null) {
                throw new ContainedException("Didn't get a last modified date for " + statements);
            }

            // Move all selected entity data statements to main entity statement
            for (Pair<URI, Literal> dataStatement: dataStatements) {
                statements.add(valueFactory.createStatement(entityUriResource,
                        dataStatement.getLeft(), dataStatement.getRight()));
            }

            statements.addAll(restoredStatements);
        }

        /**
         * Run any cleanup tasks for single label mode if we're in that mode.
         */
        private void finishSingleLabelMode() {
            if (singleLabelModeLanguages != null) {
                singleLabelModeWorkForLabel.addBestStatement(statements);
                singleLabelModeWorkForDescription.addBestStatement(statements);
            }
        }

        /**
         * Register an extra valid subject. These subjects are ok if we hit
         * them.
         */
        private void registerExtraValidSubject(String subject) {
            extraValidSubjects.add(subject);
            /*
             * Rescue any statements that may have leaked into the unknown
             * subjects due to things being in a funky order.
             */
            restoredStatements.addAll(unknownSubjects.removeAll(subject));
        }

        /**
         * Fetch the object from the current statement as a Literal.
         */
        @SuppressFBWarnings(value = "LEST_LOST_EXCEPTION_STACK_TRACE", justification = "Cause is really not needed here.")
        private Literal objectAsLiteral(Statement statement) {
            try {
                return (Literal) statement.getObject();
            } catch (ClassCastException e) {
                throw new ContainedException("Expected Literal in object position of:  " + statement);
            }
        }

        /**
         * Work for making sure we remove everything but the best label or
         * description.
         */
        private final class SingleLabelModeWork {
            /**
             * The best statement we've seen for the label/description.
             */
            private Statement bestStatement;
            /**
             * The index into the fallback list for the best statement that
             * we've seen.
             */
            private int bestIndex = -1;

            /**
             * Apply the current statement to the effort to collect the best
             * label. Single label mode is implemented by removing all labels
             * and adding the best back at the end of the munging process.
             */
            public boolean statement(Statement statement) {
                Literal object = objectAsLiteral(statement);
                String language = object.getLanguage();
                int index = singleLabelModeLanguages.indexOf(language);
                if (index > bestIndex) {
                    bestStatement = statement;
                    bestIndex = index;
                }
                return false;
            }

            /**
             * Add the best label or description to the statements if there is
             * one.
             */
            public void addBestStatement(Collection<Statement> statements) {
                if (bestStatement != null) {
                    statements.add(bestStatement);
                }
            }
        }
    }

    /**
     * Thrown when the munged triples contain a subject we don't recognize.
     */
    public static class BadSubjectException extends ContainedException {
        public BadSubjectException(Set<String> badSubjects, UrisScheme uris) {
            super(String.format(Locale.ROOT,
                    "Unrecognized subjects:  %s.  Expected only sitelinks and subjects starting with %s and %s",
                    badSubjects, uris.entityData(), uris.entityURIs()));
        }
    }
}
