package org.wikidata.query.rdf.tool.rdf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
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

/**
 * Munges RDF from Wikibase into a more queryable format. Note that this is
 * tightly coupled with Wikibase's export format.
 */
public class Munger {
    private final EntityData entityDataUris;
    private final Entity entityUris;

    public Munger(EntityData entityDataUris, Entity entityUris) {
        this.entityDataUris = entityDataUris;
        this.entityUris = entityUris;
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

        Set<String> siteLinks = new HashSet<>();
        Set<String> unknownSubjects = new HashSet<>();
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
             * We make an effort to detect subjects that we don't recognize so
             * we can report them as an error. We first have to filter out
             * sitelinks.
             */
            if (siteLinks.contains(subject)) {
                continue;
            }
            if (predicate.equals(RDF.TYPE) && s.getObject().stringValue().equals(SchemaDotOrg.ARTICLE)) {
                siteLinks.add(subject);
                // Site links may have crept into unknown subjects if they
                // appeared in a funky order.
                unknownSubjects.remove(subject);
                continue;
            }
            unknownSubjects.add(subject);
        }

        if (!unknownSubjects.isEmpty()) {
            throw new BadSubjectException(unknownSubjects, entityDataUris, entityUris);
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
