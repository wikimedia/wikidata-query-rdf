package org.wikidata.query.rdf.tool.rdf;

import static org.wikidata.query.rdf.common.uri.PropertyType.REFERENCE_VALUE;
import static org.wikidata.query.rdf.common.uri.PropertyType.REFERENCE_VALUE_NORMALIZED;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.UrisScheme;

/**
 * Set of predicates that filter rdf statements based on their
 * namespace against the ones defined by {@link UrisScheme}.
 * Consider using {@link StatementPredicates} for more efficient matching if exact URI matching is possible.
 */
public class NamespaceStatementPredicates {
    private final UrisScheme uris;

    public NamespaceStatementPredicates(UrisScheme uris) {
        this.uris = uris;
    }

    /**
     * Predicate that accepts triples whose subject belongs to the entityData NS.
     * @see UrisScheme#entityData()
     * @see UrisScheme#entityDataHttps()
     */
    public boolean subjectInEntityDataNS(Statement statement) {
        String sub = subject(statement);
        return inNamespace(sub, uris.entityData()) || inNamespace(sub, uris.entityDataHttps());
    }

    /**
     * Predicate that accepts triples whose subject belongs to the statement NS.
     * @see UrisScheme#statement()
     */
    public boolean subjectInStatementNS(Statement statement) {
        return inNamespace(subject(statement), uris.statement());
    }

    /**
     * Predicate that accepts triples whose subject belongs to the reference NS.
     * @see UrisScheme#reference()
     */
    public boolean subjectInReferenceNS(Statement statement) {
        return inNamespace(subject(statement), uris.reference());
    }

    /**
     * Predicate that accepts triples whose object belongs to the reference NS.
     * @see UrisScheme#reference()
     */
    public boolean objectInReferenceNS(Statement statement) {
        return inNamespace(object(statement), uris.reference());
    }

    /**
     * Predicate that accepts triples whose subject is a value.
     * @see UrisScheme#value() ()
     */
    public boolean subjectInValueNS(Statement statement) {
        return inNamespace(subject(statement), uris.value());
    }

    /**
     * Predicate that accepts triples whose object is a value.
     * @see UrisScheme#value() ()
     */
    public boolean objectInValueNS(Statement statement) {
        return inNamespace(object(statement), uris.value());
    }

    /**
     * Whether this triple links reference and value.
     * - subject is a reference
     * - predicate must be prv:XYZ or prn:XYZ
     * - object must be a value
     * @return Is it a ref to value link?
     */
    public boolean tripleRefValue(Statement statement) {
        // ref:XYZ Prop:RefValue|Prop:RefValueNormalized value:
        return subjectInReferenceNS(statement) && objectInValueNS(statement) &&
                (inNamespace(predicate(statement), uris.property(REFERENCE_VALUE))
                || inNamespace(predicate(statement), uris.property(REFERENCE_VALUE_NORMALIZED)));
    }

    /**
     * Whether the triple links the entity to a reified statement.
     * - subject is the entityUri provided
     * - predicate belongs to the {@link org.wikidata.query.rdf.common.uri.PropertyType#CLAIM} namespace
     * - object is in the {@link UrisScheme#statement()} namespace
     * e.g.
     *     wd:Q2 p:P1419 s:Q2-e055dd8f-4e6f-36ea-819d-5c4de88b57a0 .
     */
    public boolean reificationStatement(Statement statement) {
        return inNamespace(object(statement), uris.statement())
                && inNamespace(predicate(statement), uris.property(PropertyType.CLAIM));
    }

    private String subject(Statement statement) {
        return statement.getSubject().stringValue();
    }

    private String object(Statement statement) {
        return statement.getObject().stringValue();
    }

    private String predicate(Statement statement) {
        return statement.getPredicate().stringValue();
    }

    private static boolean inNamespace(String uri, String namespace) {
        return uri.startsWith(namespace) && uri.indexOf('/', namespace.length()) < 0;
    }

    public boolean subjectIsBNodeOrSkolemIRI(Statement statement) {
        return isBNodeOrSkolemIRI(statement.getSubject());
    }

    private boolean isBNodeOrSkolemIRI(Resource res) {
        return res instanceof BNode || (res instanceof URI && res.stringValue().startsWith(uris.wellKnownBNodeIRIPrefix()));
    }
}
