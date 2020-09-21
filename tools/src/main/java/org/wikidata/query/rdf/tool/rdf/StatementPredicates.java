package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Ontology.Geo;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.Ontology.Time;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

public final class StatementPredicates {
    private StatementPredicates() {}
    /**
     * Detects a triple using schema:softwareVersion as a predicate.
     */
    public static boolean softwareVersion(Statement statement) {
        return statement.getPredicate().stringValue().equals(SchemaDotOrg.SOFTWARE_VERSION);
    }

    /**
     * all statements with a wikibase:Dump subject.
     */
    public static boolean dumpStatement(Statement statement) {
        return statement.getSubject().stringValue().equals(Ontology.DUMP);
    }

    /**
     * Statement describing the format version of the dump.
     * wikibase:Dump schema:softwareVersion "1.0.0"
     */
    public static boolean dumpFormatVersion(Statement statement) {
        return dumpStatement(statement) && softwareVersion(statement);
    }

    /**
     * Statement reflecting a redirect.
     * wd:Q6825 owl:sameAs wd:Q26709563
     */
    public static boolean redirect(Statement statement) {
        // should we make sure that the subject and the object belong to the entity ns?
        return statement.getPredicate().stringValue().equals(OWL.SAME_AS);
    }

    /**
     * A statement declaring a type.
     * e.g.
     * subject a object
     */
    public static boolean typeStatement(Statement statement) {
        return RDF.TYPE.equals(statement.getPredicate().stringValue());
    }

    /**
     * Statement declaring a reference.
     * e.g.
     * ref:UUID a wikibase:Reference
     */
    public static boolean referenceTypeStatement(Statement statement) {
        return typeStatement(statement) && statement.getObject().stringValue().equals(Ontology.REFERENCE);
    }

    /**
     * Statement declaring a value.
     * e.g.
     * val:UUID a wikibase:QuantityValue
     */
    public static boolean valueTypeStatement(Statement statement) {
        String object = statement.getObject().stringValue();
        return typeStatement(statement) &&
                (object.equals(Quantity.TYPE) || object.equals(Time.TYPE) || object.equals(Geo.TYPE));
    }

    /**
     * Statement declaring a wikiGroup (sitelink information)
     * e.g.
     * &lt;https://en.wikipedia.org/&gt; wikbase:wikiGroup "wikipedia"
     */
    public static boolean wikiGroupDefinition(Statement statement) {
        return Ontology.WIKIGROUP.equals(statement.getPredicate().stringValue());
    }
}
