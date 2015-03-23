package org.wikidata.query.rdf.tool.rdf;

import java.util.Collection;
import java.util.Iterator;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.common.uri.Ontology;
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
    public Collection<Statement> munge(Collection<Statement> statements) {
        Iterator<Statement> itr = statements.iterator();
        Value revisionId = null;
        Value lastModified = null;
        Resource entity = null;

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
            } else if (subject.startsWith(entityUris.namespace())) {
                entity = s.getSubject();
                if (predicate.equals(RDF.TYPE) && s.getObject().stringValue().equals(Ontology.ITEM)) {
                    // We don't need wd:Q1 a wdo:item
                    itr.remove();
                }
            }
        }
        if (revisionId == null) {
            throw new RuntimeException("Didn't get a revision id!");
        }
        if (lastModified == null) {
            throw new RuntimeException("Didn't get a last modified date!");
        }
        if (entity == null) {
            throw new RuntimeException("Didn't get any entity information!");
        }
        statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.VERSION), revisionId));
        statements.add(new StatementImpl(entity, new URIImpl(SchemaDotOrg.DATE_MODIFIED), lastModified));

        return statements;
    }
}
