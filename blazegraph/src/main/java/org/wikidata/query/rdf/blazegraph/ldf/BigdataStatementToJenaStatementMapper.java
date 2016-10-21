package org.wikidata.query.rdf.blazegraph.ldf;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.util.iterator.Map1;

import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Implementation of {@link Map1} that transforms {@link BigdataStatement}s to
 * Jena {@link Statement}s.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
@SuppressWarnings("all")
public class BigdataStatementToJenaStatementMapper implements
            Map1<BigdataStatement, Statement> {
    /**
     * Jenna type mapper.
     */
    public static final TypeMapper JENA_TYPE_MAPPER = TypeMapper.getInstance();

    /**
     * Singleton instance.
     */
    private static BigdataStatementToJenaStatementMapper instance;

    /**
     * Get singleton instance.
     * @return
     */
    public static BigdataStatementToJenaStatementMapper getInstance() {
        if (instance == null) {
            instance = new BigdataStatementToJenaStatementMapper();
        }
        return instance;
    }

    @Override
    public Statement map1(final BigdataStatement blzgStmt) {
        final Resource s = convertToJenaResource(blzgStmt.getSubject());
        final Property p = convertToJenaProperty(blzgStmt.getPredicate());
        final RDFNode o = convertToJenaRDFNode(blzgStmt.getObject());

        return ResourceFactory.createStatement(s, p, o);
    }

    /**
     * Convert Bigdata resource to Jena resource.
     * @param r
     * @return Jena resource.
     */
    public Resource convertToJenaResource(final BigdataResource r) {
        return ResourceFactory.createResource(r.stringValue());
    }

    /**
     * Convert Bigdata resource to Jena property.
     * @param r
     * @return Jena property.
     */
    public Property convertToJenaProperty(final BigdataResource r) {
        return ResourceFactory.createProperty(r.stringValue());
    }

    /**
     * Convert Bigdata value to Jena RDF Node.
     * @param v
     * @return Jena RDF node.
     */
    public RDFNode convertToJenaRDFNode(final BigdataValue v) {
        if (v instanceof BigdataResource)
            return convertToJenaResource((BigdataResource) v);

        if (!(v instanceof BigdataLiteral))
            throw new IllegalArgumentException(v.getClass().getName());

        final BigdataLiteral l = (BigdataLiteral) v;
        final String lex = l.getLabel();
        final URI datatypeURI = l.getDatatype();
        final String languageTag = l.getLanguage();

        if (datatypeURI != null) {
            final RDFDatatype dt = JENA_TYPE_MAPPER
                    .getSafeTypeByName(datatypeURI.stringValue());
            return ResourceFactory.createTypedLiteral(lex, dt);
        } else if (languageTag != null) {
            return ResourceFactory.createLangLiteral(lex, languageTag);
        } else {
            return ResourceFactory.createPlainLiteral(lex);
        }
    }

}
