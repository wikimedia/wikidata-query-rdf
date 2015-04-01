package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Munges RDF from Wikibase into a more queryable format. Note that this is
 * tightly coupled with Wikibase's export format.
 */
public class Munger {
    private final EntityData entityDataUris;
    private final Entity entityUris;
    private final boolean removeSiteLinks;

    public Munger(EntityData entityDataUris, Entity entityUris) {
        this(entityDataUris, entityUris, false);
    }

    private Munger(EntityData entityDataUris, Entity entityUris, boolean removeSiteLinks) {
        this.entityDataUris = entityDataUris;
        this.entityUris = entityUris;
        this.removeSiteLinks = removeSiteLinks;
    }

    /**
     * Build a Munger that removes site links.
     */
    public Munger removeSiteLinks() {
        return new Munger(entityDataUris, entityUris, true);
    }

    /**
     * Adds and removes entries from the statements collection to munge Wikibase
     * RDF exports into a more queryable form.
     *
     * @param statements statements to munge
     * @return a reference to statements
     */
    public Collection<Statement> munge(String entityId, Collection<Statement> statements) {
        if (statements.isEmpty()) {
            // Empty collection is a delete.
            return statements;
        }
        // Filters and adds RDF based in a single pass.
        Iterator<Statement> itr = statements.iterator();
        String entityUri = entityUris.namespace() + entityId;
        Value revisionId = null;
        Value lastModified = null;
        Resource entity = null;

        /*
         * A list of statements that were removed from the original collection
         * in error.
         */
        List<Statement> restoredStatements = new ArrayList<>();
        /*
         * Subject of all sitelinks.
         */
        Set<String> siteLinks = new HashSet<>();
        /*
         * Subjects that likely showed up in statements in error. If a later
         * statement merits the re-inclusion of the subject then its statements
         * will be removed from this multimap and added to restoredStatement.
         */
        ListMultimap<String, Statement> unknownSubjects = ArrayListMultimap.create();
        while (itr.hasNext()) {
            Statement s = itr.next();
            String subject = s.getSubject().stringValue();
            String predicate = s.getPredicate().stringValue();
            if (subject.startsWith(entityDataUris.namespace())) {
                if (revisionId == null && predicate.equals(SchemaDotOrg.VERSION)) {
                    revisionId = s.getObject();
                } else if (lastModified == null && predicate.equals(SchemaDotOrg.DATE_MODIFIED)) {
                    lastModified = s.getObject();
                } else if (entity == null && predicate.equalsIgnoreCase(SchemaDotOrg.ABOUT)) {
                    try {
                        entity = (Resource) s.getObject();
                    } catch (ClassCastException e) {
                        throw new RuntimeException("Unexepect object with schema:about predicate.  "
                                + "Expected data:Q<foo> schema:about entity:Q<foo>", e);
                    }
                }
                itr.remove();
                continue;
            }
            if (subject.startsWith(entityUris.namespace())) {
                entity = s.getSubject();
                if (!subject.equals(entityUri)) {
                    /*
                     * Some flavors of rdf dump information about other entities
                     * along side the main entity. We can't handle that properly
                     * and it doesn't make a ton of sense anyway.
                     */
                    itr.remove();
                } else if (predicate.equals(RDF.TYPE) && s.getObject().stringValue().equals(Ontology.ITEM)) {
                    // We don't need wd:Q1 a wdo:item
                    itr.remove();
                } else if (predicate.equals(SchemaDotOrg.NAME)) {
                    // Q1 schema:name "foo" is a dupe of rdfs:label
                    itr.remove();
                } else if (predicate.equals(SKOS.PREF_LABEL)) {
                    // Q1 skos:prefLabel "foo" is a dupe of rdfs:label
                    itr.remove();
                }
                continue;
            }
            /*
             * Detecting site links is important so we can (optionally) filter
             * them out and so that we can report everything that isn't a
             * sitelink or proper subject as an error.
             */
            if (siteLinks.contains(subject)) {
                if (removeSiteLinks) {
                    itr.remove();
                }
                continue;
            }
            if (predicate.equals(RDF.TYPE) && s.getObject().stringValue().equals(SchemaDotOrg.ARTICLE)) {
                siteLinks.add(subject);
                // Site links may have crept into unknown subjects if they
                // appeared in a funky order.
                if (removeSiteLinks) {
                    itr.remove();
                    unknownSubjects.removeAll(subject);
                } else {
                    restoredStatements.addAll(unknownSubjects.removeAll(subject));
                }
                continue;
            }
            unknownSubjects.put(subject, s);
            itr.remove();
        }

        if (!unknownSubjects.isEmpty()) {
            throw new BadSubjectException(unknownSubjects.keySet(), entityDataUris, entityUris);
        }
        if (revisionId == null) {
            throw new RuntimeException("Didn't get a revision id for " + statements);
        }
        if (lastModified == null) {
            throw new RuntimeException("Didn't get a last modified date for " + statements);
        }
        if (entity == null) {
            throw new RuntimeException("Didn't get any entity information " + statements);
        }
        statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.VERSION), revisionId));
        statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.DATE_MODIFIED), lastModified));
        statements.addAll(restoredStatements);
        return statements;
    }

    public class BadSubjectException extends RuntimeException {
        private static final long serialVersionUID = -4869053066714948939L;

        public BadSubjectException(Set<String> badSubjects, EntityData entityDataUris, Entity entityUris) {
            super(String.format(Locale.ROOT,
                    "Unrecognized subjects:  %s.  Expected only sitelinks and subjects starting with %s and %s",
                    badSubjects, entityDataUris.namespace(), entityUris.namespace()));
        }
    }
}
