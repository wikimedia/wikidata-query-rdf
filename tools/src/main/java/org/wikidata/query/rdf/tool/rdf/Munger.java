package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.WikibasePoint.CoordinateOrder;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Ontology.Lexeme;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Munges RDF from Wikibase into a more queryable format. Note that this is
 * tightly coupled with Wikibase's export format.
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class Munger {
    private static final Logger log = LoggerFactory.getLogger(Munger.class);

    /**
     * Wikibase uris we're working with.
     */
    private final WikibaseUris uris;
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
    private boolean keepTypes;

    /**
     * Format version we're dealing with.
     */
    private String dumpFormatVersion;

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
     * Types that we will remove from data.
     */
    private static final Set<String> SKIPPED_TYPES = ImmutableSet
            .of(Ontology.ITEM, Lexeme.LEXEME, Lexeme.FORM, Lexeme.SENSE);

    /**
     * Map of format handlers.
     */
    private final Map<String, FormatHandler> formatHandlers;

    public Munger(WikibaseUris uris) {
        this(uris, null, null, false);
    }

    private Munger(WikibaseUris uris, Set<String> limitLabelLanguages, List<String> singleLabelModeLanguages,
            boolean removeSiteLinks) {
        this.uris = uris;
        this.limitLabelLanguages = limitLabelLanguages;
        this.singleLabelModeLanguages = singleLabelModeLanguages;
        this.removeSiteLinks = removeSiteLinks;
        this.formatHandlers = new HashMap<>();

        // 0.0.1 has format lat-long, 0.0.2 has format long-lat
        // Depending on which default we have now, we want to switch one of them
        if (WikibasePoint.DEFAULT_ORDER == CoordinateOrder.LAT_LONG) {
            addFormatHandler("0.0.2", new PointCoordinateSwitcher());
        } else {
            addFormatHandler("0.0.1", new PointCoordinateSwitcher());
        }
    }

    /**
     * Set the keep types parameter.
     */
    public Munger keepTypes(boolean keep) {
        keepTypes = keep;
        return this;
    }

    /**
     * Build a Munger that only imports labels in some languages.
     */
    public Munger limitLabelLanguages(String... languages) {
        return limitLabelLanguages(Arrays.asList(languages));
    }

    /**
     * Build a Munger that only imports labels in some languages.
     */
    public Munger limitLabelLanguages(Collection<String> languages) {
        return new Munger(uris, ImmutableSet.copyOf(languages), singleLabelModeLanguages, removeSiteLinks);
    }

    /**
     * Build a munger that will load only a single label per entity. Note that
     * if there isn't a label in one of the languages then there will be no
     * label for the entity.
     *
     * @param languages a fallback chain of languages with the first one being
     *            the most important
     */
    public Munger singleLabelMode(String... languages) {
        return singleLabelMode(Arrays.asList(languages));
    }

    /**
     * Build a munger that will load only a single label per entity.
     *
     * @param languages a fallback chain of languages with the first one being
     *            the most important
     */
    public Munger singleLabelMode(Collection<String> languages) {
        return new Munger(uris, limitLabelLanguages, ImmutableList.copyOf(languages).reverse(), removeSiteLinks);
    }

    /**
     * Build a Munger that removes site links.
     */
    public Munger removeSiteLinks() {
        return new Munger(uris, limitLabelLanguages, singleLabelModeLanguages, true);
    }

    /**
     * Set format version.
     */
    public void setFormatVersion(String version) {
        this.dumpFormatVersion = version;
    }

    /**
     * Add handler for specific non-default format.
     * @param version Version to handle.
     * @param handler Handler.
     */
    public final void addFormatHandler(String version, FormatHandler handler) {
        formatHandlers.put(version, handler);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     * @param existingValues Existing value statements
     * @param existingRefs Existing reference statements
     * @param sourceChange Change that originated the operation
     */
    public void munge(String entityId, Collection<Statement> statements, Collection<String> existingValues,
            Collection<String> existingRefs, Change sourceChange) {

        if (statements.isEmpty()) {
            // Empty collection is a delete.
            return;
        }
        MungeOperation op = new MungeOperation(entityId, statements, existingValues, existingRefs);
        if (sourceChange != null) {
            op.importFromChange(sourceChange);
        }
        op.munge();
        if (sourceChange != null) {
            final long sourceRev = sourceChange.revision();
            final long fetchedRev = op.getRevisionId();
            if (sourceRev > 0 && fetchedRev <  sourceRev) {
                // Something weird happened - we've got stale revision!
                log.warn("Stale revision on {}: change is {}, RDF is {}", entityId, sourceRev, fetchedRev);
            }
        }
        // remove all values that we have seen as they are used by statements
        existingValues.removeAll(op.extraValidSubjects);
        existingRefs.removeAll(op.extraValidSubjects);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * This variant also extracts value and reference nodes from multimap
     * collected by Updater and the ones that will be left in the container
     * at the end of the operation are unused by this change and have to be
     * cleaned up.
     *
     * @param statements statements to munge
     * @param repoValues multimap of all value nodes, keyed by entity ID
     * @param repoValues multimap of all reference nodes, keyed by entity ID
     * @param valuesContainer Value nodes container
     * @param refsContainer Reference nodes container
     * @param sourceChange Change that originated the operation
     */
    public void mungeWithValues(String entityId,
            Collection<Statement> statements,
            Multimap<String, String> repoValues,
            Multimap<String, String> repoRefs,
            Collection<String> valuesContainer,
            Collection<String> refsContainer,
            Change sourceChange) {
        valuesContainer.addAll(repoValues.get(uris.entity() + entityId));
        refsContainer.addAll(repoRefs.get(uris.entity() + entityId));
        munge(entityId, statements, valuesContainer, refsContainer, sourceChange);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     * @param existingValues Existing value statements
     * @param existingRefs Existing reference statements
     */
    public void munge(String entityId, Collection<Statement> statements, Collection<String> existingValues,
            Collection<String> existingRefs) {
        munge(entityId, statements, existingValues, existingRefs, null);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     */
    @SuppressWarnings("unchecked")
    public void munge(String entityId, Collection<Statement> statements) {
        munge(entityId, statements, Collections.EMPTY_SET, Collections.EMPTY_SET, null);
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
        private final Resource entityUriImpl;

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
         * Existing values that we'll just remove from the provided statements.
         */
        private final Collection<String> existingValues;
        /**
         * Existing references which we'll just remove from the provided
         * statements.
         */
        private final Collection<String> existingRefs;

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
         * The current statement being processed.
         */
        private Statement statement;
        /**
         * The subject of the statement being processed.
         */
        private String subject;
        /**
         * The predicate of the statement being processed.
         */
        private String predicate;
        /**
         * Format handler for current format.
         */
        private FormatHandler formatHandler;
        /**
         * Set of statements that have no rank statement.
         */
        private Set<String> statementsWithoutRanks = new HashSet<>();
        /**
         * Set of statements that have rank statement.
         */
        private Set<String> statementsWithRanks = new HashSet<>();


        MungeOperation(String entityId, Collection<Statement> statements, Collection<String> existingValues,
                Collection<String> existingRefs) {
            this.statements = statements;
            entityUri = uris.entity() + entityId;
            entityUriImpl = new URIImpl(entityUri);
            if (singleLabelModeLanguages != null) {
                singleLabelModeWorkForLabel = new SingleLabelModeWork();
                singleLabelModeWorkForDescription = new SingleLabelModeWork();
            } else {
                singleLabelModeWorkForLabel = null;
                singleLabelModeWorkForDescription = null;
            }
            this.existingValues = existingValues;
            this.existingRefs = existingRefs;
            setFormatVersion(dumpFormatVersion);
        }

        /**
         * Set current version of the format.
         * @param version
         */
        private void setFormatVersion(String version) {
            this.formatHandler = formatHandlers.get(version);
        }

        /**
         * Import revision data from Change object.
         * @param sourceChange
         */
        public void importFromChange(Change sourceChange) {
            if (sourceChange.revision() > 0) {
                this.revisionId = new NumericLiteralImpl(sourceChange.revision());
            }
            if (sourceChange.timestamp() != null) {
                this.lastModified = new LiteralImpl(sourceChange.timestamp().toString(), XMLSchema.DATETIME);
            }
        }

        /**
         * Get revision ID for this change.
         * @return
         */
        public long getRevisionId() {
            return revisionId == null ? -1 : Long.parseLong(revisionId.stringValue());
        }

        /**
         * Munge the statements.
         */
        public void munge() {
            Iterator<Statement> itr = statements.iterator();
            while (itr.hasNext()) {
                statement = itr.next();
                if (formatHandler != null) {
                    Statement handled = formatHandler.handle(statement);
                    if (handled == null) {
                        // drop it
                        itr.remove();
                        continue;
                    } else {
                        if (!handled.equals(statement)) {
                            // modified
                            itr.remove();
                            statement = handled;
                            if (statement()) {
                                // if we accept it in modified form, add back
                                restoredStatements.add(statement);
                                continue;
                            }
                        }
                    }
                }
                if (!statement()) {
                    itr.remove();
                }
                // Check object length, cut if needed.
                final Statement shortStatement = checkObjectLength();
                if (shortStatement != null) {
                    itr.remove();
                    restoredStatements.add(shortStatement);
                }
            }

            statement = null;
            finishSingleLabelMode();
            finishCommon();
        }

        /**
         * Check whether object's length is more than 32k.
         * If so, create new statement that cuts object down to 32k.
         * @return New statement or null if not needed.
         */
        private Statement checkObjectLength() {
            if (statement.getObject() instanceof Literal) {
                final Literal value = (Literal)statement.getObject();
                if (value.stringValue().length() > Short.MAX_VALUE) {
                    final Literal newValue;
                    if (value.getDatatype().equals(org.openrdf.model.vocabulary.RDF.LANGSTRING)) {
                        newValue = new LiteralImpl(value.stringValue().substring(0, Short.MAX_VALUE), value.getLanguage());
                    } else {
                        newValue = new LiteralImpl(value.stringValue().substring(0, Short.MAX_VALUE), value.getDatatype());
                    }
                    return new StatementImpl(statement.getSubject(),
                            statement.getPredicate(), newValue);
                }
            }
            return null;
        }

        /**
         * Process a statement.
         *
         * @return true to keep the statement, false to remove it
         */
        @SuppressWarnings("checkstyle:npathcomplexity")
        private boolean statement() {
            subject = statement.getSubject().stringValue();
            predicate = statement.getPredicate().stringValue();
            if (subject.equals(Ontology.DUMP)) {
                // temporary patch for T98405
                return false;
            }
            if (inNamespace(subject, uris.entityData()) || inNamespace(subject, uris.entityDataHttps())) {
                return entityDataStatement();
            }
            if (inNamespace(subject, uris.statement())) {
                return entityStatementStatement();
            }
            if (inNamespace(subject, uris.reference())) {
                return entityReferenceStatement();
            }
            if (inNamespace(subject, uris.value())) {
                return entityValueStatement();
            }
            if (inNamespace(subject, uris.entity())) {
                return entityStatement();
            }
            if (subject.startsWith(uris.property(PropertyType.CLAIM))) {
                return propertyStatement();
            }
            /*
             *  Allow bnodes, they are not linked to specific entity
             *  but used to declare classes
             */
            if (statement.getSubject() instanceof BNode) {
                return true;
            }
            return unknownStatement();
        }

        /**
         * Is a uri in just this namespace? The trouble is that some namespaces
         * are suffixes of one another. For now / isn't a valid character after
         * a namespace so we can use its absence to determine that we're just in
         * the provided namespace and not a suffix namespace.
         */
        private boolean inNamespace(String uri, String namespace) {
            return uri.startsWith(namespace) && uri.indexOf('/', namespace.length()) < 0;
        }

        /**
         * Process statement about property.
         * @return true to keep the statement, false to remove it
         */
        private boolean propertyStatement() {
            // This is wdno:P123 a owlClass, owl:complementOf _:blah - allow it
            if (subject.startsWith(uris.property(PropertyType.NOVALUE))) {
                return true;
            }
            // It's p:P2762 a owl:ObjectProperty, it's ok.
            if (predicate.equals(RDF.TYPE)) {
                return true;
            }
            return false;
        }


        /**
         * Process a statement who's subject is in the entityData prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityDataStatement() {
            boolean knownPredicate = false;
            // Three specific ones are recorded for later re-application
            switch (predicate) {
            case SchemaDotOrg.VERSION:
                knownPredicate = true;
                revisionId = objectAsLiteral();
                break;
            case SchemaDotOrg.DATE_MODIFIED:
                knownPredicate = true;
                lastModified = objectAsLiteral();
                break;
            case SchemaDotOrg.SOFTWARE_VERSION:
                setFormatVersion(objectAsLiteral().stringValue());
                break;
            default:
                if (predicate.startsWith(Ontology.NAMESPACE)) {
                    knownPredicate = true;
                }
                    // Noop - fall out is ok as we just remove them.
            }
            if (knownPredicate) {
                dataStatements.add(new ImmutablePair<URI, Literal>(
                        statement.getPredicate(), objectAsLiteral()));
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
        private boolean entityStatement() {
            if (!subject.equals(entityUri) && !subEntities.contains(subject)) {
                /*
                 * Some flavors of rdf dump information about other entities
                 * along side the main entity. We can't handle that properly and
                 * it doesn't make a ton of sense anyway.
                 */
                return false;
            }
            switch (predicate) {
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
                    if (subject.startsWith(uris.entity() + "L")
                            || subEntities.contains(subject)) {
                    // Skip labels for Lexeme & its sub-entities, e.g. Forms and Senses
                    return false;
                }
                return limitLabelLanguage() && singleLabelMode(singleLabelModeWorkForLabel);
            case SchemaDotOrg.DESCRIPTION:
                return limitLabelLanguage() && singleLabelMode(singleLabelModeWorkForDescription);
            case SKOS.ALT_LABEL:
                return limitLabelLanguage();
            case OWL.SAME_AS:
                return true;
            case Ontolex.LEXICAL_FORM:
            case Ontolex.SENSE_PREDICATE:
                // Links to Form and Sense
                subEntities.add(statement.getObject().stringValue());
                return true;
            default:
                return entityStatementWithUnrecognizedPredicate();
            }
        }

        /**
         * Process a statement about an entity who's predicate isn't explicitly
         * recognized.
         */
        private boolean entityStatementWithUnrecognizedPredicate() {
            String object = statement.getObject().stringValue();
            if (inNamespace(object, uris.statement())) {
                // Register statement for rank checking
                statementsWithoutRanks.add(object);
            }
            if (inNamespace(predicate, uris.property(PropertyType.CLAIM)) && inNamespace(object, uris.statement())) {
                registerExtraValidSubject(object);
            }
            // Most statements should be kept.
            return true;
        }

        /**
         * Process a statement who's subject is in the entity statement prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityStatementStatement() {
            switch (predicate) {
            case RDF.TYPE:
                // Haven't seen the rank yet
                statementsWithoutRanks.add(subject);
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
                String object = statement.getObject().stringValue();
                if (inNamespace(object, uris.reference())) {
                    registerExtraValidSubject(object);
                }
                return true;
            case Ontology.RANK:
                statementsWithRanks.add(subject);
                break;
            default:
            }
            if (!extraValidSubjects.contains(subject)) {
                /*
                 * Remove any statements about statements that we don't yet know
                 * are actually linked to the entity. We keep a backup of them
                 * in unknownSubjects so we can restore them if they are later
                 * linked.
                 */
                unknownSubjects.put(subject, statement);
                return false;
            }
            String object = statement.getObject().stringValue();
            if (inNamespace(object, uris.value())) {
                registerExtraValidSubject(object);
            }
            return true;
        }

        /**
         * Whether this triple links reference and value.
         * @param object Statement object as String.
         * @return Is it ref->value?
         */
        private boolean tripleRefValue(String object) {
            if (!inNamespace(object, uris.value())) {
                return false;
            }
            if (inNamespace(predicate, uris.property(PropertyType.REFERENCE_VALUE)) ||
                    inNamespace(predicate, uris.property(PropertyType.REFERENCE_VALUE_NORMALIZED))) {
                return true;
            }
            return false;
        }

        /**
         * Process a statement who's subject is in the entity reference prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityReferenceStatement() {
            String object = statement.getObject().stringValue();
            if (existingRefs.contains(subject)) {
                if (tripleRefValue(object) && !existingValues.contains(object)) {
                    /* Something is wrong here: we know this ref but somehow don't know its value.
                     * We should recover it.
                     */
                    registerExtraValidSubject(object);
                    log.info("Weird reference {}: unknown value {} for {}", subject, object, entityUri);
                }

                /*
                 * TODO: we temporarily keep all the ref data because of the issues
                 * in https://phabricator.wikimedia.org/T194325
                 * We already have this ref, so no need to import it again since
                 * refs are IDed by content, we know it is the same
                 */
                // return false;
            }
            switch (predicate) {
            case RDF.TYPE:
                if (keepTypes) {
                    return true;
                }
                /*
                 * We don't need r:<uuid> a ontology:Reference because its super
                 * common and not super interesting.
                 */
                if (statement.getObject().stringValue().equals(Ontology.REFERENCE)) {
                    return false;
                }
                break;
            default:
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
            if (tripleRefValue(object)) {
                registerExtraValidSubject(object);
            }
            return true;
        }

        /**
         * Process a statement who's subject is in the entity value prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityValueStatement() {
            if (existingValues.contains(subject)) {
                /*
                 * We already have this value, so no need to import it again
                 * Since values are IDed by content, we know it is the same
                 */
                return false;
            }
            switch (predicate) {
            case RDF.TYPE:
                if (keepTypes) {
                    return true;
                }
                /*
                 * We don't need v:<uuid> a ontology:Value because its super
                 * common and not super interesting.
                 */
                if (statement.getObject().stringValue().equals(Ontology.VALUE)) {
                    return false;
                }
                break;
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
        private boolean unknownStatement() {
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
        private boolean limitLabelLanguage() {
            if (limitLabelLanguages == null) {
                return true;
            }
            Literal object = objectAsLiteral();
            String language = object.getLanguage();
            return language != null && limitLabelLanguages.contains(language);
        }

        /**
         * Handle a label or statement in single label mode or just keep the
         * label if we're not in single label mode.
         */
        private boolean singleLabelMode(SingleLabelModeWork work) {
            return work == null ? true : work.statement();
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
                    log.info(
                            "Unrecognized subjects: {} while processing {}.  Expected only sitelinks and subjects starting with {} and {}",
                            unknownSubjects.keySet(), entityUri, uris.entityData(), uris.entity());
                }
            }
            statementsWithoutRanks.removeAll(statementsWithRanks);
            if (!statementsWithoutRanks.isEmpty()) {
                /**
                 * We have some statements without ranks, this is very weird.
                 */
                log.error(
                        "Found some statements without ranks while processing {}: {}",
                        entityUri, statementsWithoutRanks);
            }
            if (revisionId == null) {
                throw new ContainedException("Didn't get a revision id for " + statements);
            }
            if (lastModified == null) {
                throw new ContainedException("Didn't get a last modified date for " + statements);
            }

            // Move all selected entity data statements to main entity statement
            for (Pair<URI, Literal> dataStatement: dataStatements) {
                statements.add(new StatementImpl(entityUriImpl,
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
        private Literal objectAsLiteral() {
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
        private class SingleLabelModeWork {
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
            public boolean statement() {
                Literal object = objectAsLiteral();
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
        private static final long serialVersionUID = -4869053066714948939L;

        public BadSubjectException(Set<String> badSubjects, WikibaseUris uris) {
            super(String.format(Locale.ROOT,
                    "Unrecognized subjects:  %s.  Expected only sitelinks and subjects starting with %s and %s",
                    badSubjects, uris.entityData(), uris.entity()));
        }
    }
}
