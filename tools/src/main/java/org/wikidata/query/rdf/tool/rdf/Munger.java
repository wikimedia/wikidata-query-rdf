package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.tool.exception.ContainedException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

/**
 * Munges RDF from Wikibase into a more queryable format. Note that this is
 * tightly coupled with Wikibase's export format.
 */
public class Munger {
    private final EntityData entityDataUris;
    private final Entity entityUris;
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

    public Munger(EntityData entityDataUris, Entity entityUris) {
        this(entityDataUris, entityUris, null, null, false);
    }

    private Munger(EntityData entityDataUris, Entity entityUris, Set<String> limitLabelLanguages,
            List<String> singleLabelModeLanguages, boolean removeSiteLinks) {
        this.entityDataUris = entityDataUris;
        this.entityUris = entityUris;
        this.limitLabelLanguages = limitLabelLanguages;
        this.singleLabelModeLanguages = singleLabelModeLanguages;
        this.removeSiteLinks = removeSiteLinks;
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
        return new Munger(entityDataUris, entityUris, ImmutableSet.copyOf(languages), singleLabelModeLanguages,
                removeSiteLinks);
    }

    /**
     * Build a munger that will load only a single label per entity.
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
        return new Munger(entityDataUris, entityUris, limitLabelLanguages, ImmutableList.copyOf(languages).reverse(),
                removeSiteLinks);
    }

    /**
     * Build a Munger that removes site links.
     */
    public Munger removeSiteLinks() {
        return new Munger(entityDataUris, entityUris, limitLabelLanguages, singleLabelModeLanguages, true);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     * @return a reference to statements
     */
    public Collection<Statement> munge(String entityId, Collection<Statement> statements) throws ContainedException {
        if (statements.isEmpty()) {
            // Empty collection is a delete.
            return statements;
        }
        MungeOperation op = new MungeOperation(entityId, statements);
        op.munge();
        return op.statements;
    }

    /**
     * Holds state during a single munge operation.
     */
    private class MungeOperation {
        private final String entityUri;
        private final Collection<Statement> statements;

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
         * Subject of all sitelinks.
         */
        private final Set<String> siteLinks = new HashSet<>();
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

        // These are set by the entire munge operation
        private Literal revisionId = null;
        private Literal lastModified = null;
        private Resource entity = null;

        // These are setup by munge and reset for every statement
        private Statement statement;
        private String subject;
        private String predicate;

        public MungeOperation(String entityId, Collection<Statement> statements) {
            this.statements = statements;
            entityUri = entityUris.namespace() + entityId;
            if (singleLabelModeLanguages != null) {
                singleLabelModeWorkForLabel = new SingleLabelModeWork();
                singleLabelModeWorkForDescription = new SingleLabelModeWork();
            } else {
                singleLabelModeWorkForLabel = null;
                singleLabelModeWorkForDescription = null;
            }
        }

        public void munge() throws ContainedException {
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
        private boolean statement() throws ContainedException {
            subject = statement.getSubject().stringValue();
            predicate = statement.getPredicate().stringValue();
            if (subject.startsWith(entityDataUris.namespace())) {
                return entityDataStatement();
            }
            if (subject.startsWith(entityUris.namespace())) {
                return entityStatement();
            }
            return unknownStatement();
        }

        /**
         * Process a statement who's subject is in the entityData prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityDataStatement() throws ContainedException {
            // Three specific ones are recorded for later re-application
            switch (predicate) {
            case SchemaDotOrg.VERSION:
                revisionId = objectAsLiteral();
                break;
            case SchemaDotOrg.DATE_MODIFIED:
                lastModified = objectAsLiteral();
                break;
            case SchemaDotOrg.ABOUT:
                entity = objectAsResource();
                break;
            default:
                // Noop - fall out is ok as we just remove them.
            }
            // All EntityDate statements are removed.
            return false;
        }

        /**
         * Process a statement who's subject is in the entity prefix.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean entityStatement() throws ContainedException {
            if (!subject.equals(entityUri)) {
                /*
                 * Some flavors of rdf dump information about other entities
                 * along side the main entity. We can't handle that properly and
                 * it doesn't make a ton of sense anyway.
                 */
                return false;
            }
            entity = statement.getSubject();
            switch (predicate) {
            case RDF.TYPE:
                /*
                 * We don't need wd:Q1 a ontology:item because its true of
                 * almost 100% of the entities in the triple store.
                 */
                return statement.getObject().stringValue().equals(Ontology.ITEM);
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
            default:
                // Most statements should be kept.
                return true;
            }
        }

        /**
         * Process a statement who's subject isn't in the entityData or entity
         * prefixes.
         *
         * @return true to keep the statement, false to remove it
         */
        private boolean unknownStatement() {
            if (siteLinks.contains(subject)) {
                return !removeSiteLinks;
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
        private boolean limitLabelLanguage() throws ContainedException {
            if (limitLabelLanguages == null) {
                return true;
            }
            Literal object = objectAsLiteral();
            String language = object.getLanguage();
            return language != null && limitLabelLanguages.contains(language);
        }

        private boolean singleLabelMode(SingleLabelModeWork work) throws ContainedException {
            return work == null ? true : work.statement();
        }

        /**
         * Perform all munge completion tasks that are required no matter the
         * configuration. Its important that finishCommon be the last finish
         * method called because it is the one that restores restoredStatments
         * into the original statements collection.
         */
        private void finishCommon() throws ContainedException {
            if (!unknownSubjects.isEmpty()) {
                throw new BadSubjectException(unknownSubjects.keySet(), entityDataUris, entityUris);
            }
            if (revisionId == null) {
                throw new ContainedException("Didn't get a revision id for " + statements);
            }
            if (lastModified == null) {
                throw new ContainedException("Didn't get a last modified date for " + statements);
            }
            if (entity == null) {
                throw new ContainedException("Didn't get any entity information " + statements);
            }
            statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.VERSION), revisionId));
            statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.DATE_MODIFIED), lastModified));
            statements.addAll(restoredStatements);
        }

        /**
         * Run any cleanup tasks for single label mode if we're in that mode.
         */
        private void finishSingleLabelMode() {
            if (singleLabelModeLanguages != null) {
                statements.add(singleLabelModeWorkForLabel.collectBestStatement());
                statements.add(singleLabelModeWorkForDescription.collectBestStatement());
            }
        }

        /**
         * Fetch the object from the current statement as a Resource.
         *
         * @throws ContainedException if the object isn't a Resource
         */
        private Resource objectAsResource() throws ContainedException {
            try {
                return (Resource) statement.getObject();
            } catch (ClassCastException e) {
                throw new ContainedException("Unexpected Resource in object position of:  " + statement);
            }
        }

        /**
         * Fetch the object from the current statement as a Literal.
         *
         * @throws ContainedException if the object isn't a Literal
         */
        private Literal objectAsLiteral() throws ContainedException {
            try {
                return (Literal) statement.getObject();
            } catch (ClassCastException e) {
                throw new ContainedException("Unexpected Literal in object position of:  " + statement);
            }
        }

        private class SingleLabelModeWork {
            private Statement bestStatement = null;
            private int bestIndex = -1;

            /**
             * Apply the current statement to the effort to collect the best
             * label. Single label mode is implemented by removing all labels
             * and adding the best back at the end of the munging process.
             */
            public boolean statement() throws ContainedException {
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
             * Collect the best statement at the end of processing the labels.
             */
            public Statement collectBestStatement() {
                if (bestStatement == null) {
                    return new StatementImpl(entity, new URIImpl(RDFS.LABEL), entity);
                } else {
                    return bestStatement;
                }
            }
        }
    }

    public class BadSubjectException extends ContainedException {
        private static final long serialVersionUID = -4869053066714948939L;

        public BadSubjectException(Set<String> badSubjects, EntityData entityDataUris, Entity entityUris) {
            super(String.format(Locale.ROOT,
                    "Unrecognized subjects:  %s.  Expected only sitelinks and subjects starting with %s and %s",
                    badSubjects, entityDataUris.namespace(), entityUris.namespace()));
        }
    }
}
