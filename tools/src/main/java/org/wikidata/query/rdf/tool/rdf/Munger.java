package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
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

    public Munger(WikibaseUris uris) {
        this(uris, null, null, false);
    }

    private Munger(WikibaseUris uris, Set<String> limitLabelLanguages, List<String> singleLabelModeLanguages,
            boolean removeSiteLinks) {
        this.uris = uris;
        this.limitLabelLanguages = limitLabelLanguages;
        this.singleLabelModeLanguages = singleLabelModeLanguages;
        this.removeSiteLinks = removeSiteLinks;
    }

    /**
     * Set the keep types parameter.
     * @param keep
     * @return
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
        // remove all values that we have seen as they are used by statements
        existingValues.removeAll(op.extraValidSubjects);
        existingRefs.removeAll(op.extraValidSubjects);
        return;
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

        public MungeOperation(String entityId, Collection<Statement> statements, Collection<String> existingValues,
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
                Date date = sourceChange.timestamp();
                Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
                c.setTime(date);
                this.lastModified = new LiteralImpl(DatatypeConverter.printDate(c), XMLSchema.DATETIME);
            }
        }

        /**
         * Munge the statements.
         */
        public void munge() {
            Iterator<Statement> itr = statements.iterator();
            while (itr.hasNext()) {
                statement = itr.next();
                if (!statement()) {
                    itr.remove();
                }
            }

            statement = null;
            finishSingleLabelMode();
            finishCommon();
        }

        /**
         * Process a statement.
         *
         * @return true to keep the statement, false to remove it
         */
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
         * Process a statement who's subject is in the entityData prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityDataStatement() {
            // Three specific ones are recorded for later re-application
            switch (predicate) {
            case SchemaDotOrg.VERSION:
                revisionId = objectAsLiteral();
                break;
            case SchemaDotOrg.DATE_MODIFIED:
                lastModified = objectAsLiteral();
                break;
            default:
                // Noop - fall out is ok as we just remove them.
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
            if (!subject.equals(entityUri)) {
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
                return !statement.getObject().stringValue().equals(Ontology.ITEM);
            case SchemaDotOrg.NAME:
            case SKOS.PREF_LABEL:
                // Q1 schema:name "foo" is a dupe of rdfs:label
                // Q1 skos:prefLabel "foo" is a dupe of rdfs:label
                return false;
            case RDFS.LABEL:
                return limitLabelLanguage() && singleLabelMode(singleLabelModeWorkForLabel);
            case SchemaDotOrg.DESCRIPTION:
                return limitLabelLanguage() && singleLabelMode(singleLabelModeWorkForDescription);
            case SKOS.ALT_LABEL:
                return limitLabelLanguage();
            case OWL.SAME_AS:
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
         * Process a statement who's subject is in the entity reference prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityReferenceStatement() {
            if (existingRefs.contains(subject)) {
                /*
                 * We already have this ref, so no need to import it again aince
                 * refs are IDed by content, we know it is the same
                 */
                return false;
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
            String object = statement.getObject().stringValue();
            if (inNamespace(predicate, uris.property(PropertyType.REFERENCE_VALUE)) && inNamespace(object, uris.value())) {
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
            // This is wdno:P123 a owlClass, owl:complementOf _:blah - allow it
            if (subject.startsWith(uris.property(PropertyType.NOVALUE))) {
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
                    log.debug(
                            "Unrecognized subjects: {}.  Expected only sitelinks and subjects starting with {} and {}",
                            unknownSubjects.keySet(), uris.entityData(), uris.entity());
                }
            }
            if (revisionId == null) {
                throw new ContainedException("Didn't get a revision id for " + statements);
            }
            if (lastModified == null) {
                throw new ContainedException("Didn't get a last modified date for " + statements);
            }
            statements.add(new StatementImpl(entityUriImpl, new URIImpl(SchemaDotOrg.VERSION), revisionId));
            statements.add(new StatementImpl(entityUriImpl, new URIImpl(SchemaDotOrg.DATE_MODIFIED), lastModified));
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
        private Literal objectAsLiteral() {
            try {
                return (Literal) statement.getObject();
            } catch (ClassCastException e) {
                throw new ContainedException("Unexpected Literal in object position of:  " + statement);
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
    public class BadSubjectException extends ContainedException {
        private static final long serialVersionUID = -4869053066714948939L;

        public BadSubjectException(Set<String> badSubjects, WikibaseUris uris) {
            super(String.format(Locale.ROOT,
                    "Unrecognized subjects:  %s.  Expected only sitelinks and subjects starting with %s and %s",
                    badSubjects, uris.entityData(), uris.entity()));
        }
    }
}
