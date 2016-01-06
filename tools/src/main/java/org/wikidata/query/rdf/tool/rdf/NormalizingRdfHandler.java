package org.wikidata.query.rdf.tool.rdf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.wikidata.query.rdf.common.uri.Ontology;

/**
 * An RDFHandler that wraps another handler normalizing any of the (currently)
 * rather different wikidata output forms into a single form.
 */
public class NormalizingRdfHandler extends DelegatingRdfHandler {
    public NormalizingRdfHandler(RDFHandler next) {
        super(next);
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        if (uri.contains("ontology-0.0.1")) {
            uri = uri.replace("ontology-0.0.1", "ontology");
        }
        if (uri.contains("ontology-beta")) {
            uri = uri.replace("ontology-beta", "ontology");
        }
        if (uri.startsWith(Ontology.OLD_NAMESPACE)) {
            uri = uri.replace(Ontology.OLD_NAMESPACE, Ontology.NAMESPACE);
        }
        super.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        Resource subject = statement.getSubject();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();

        if (subject instanceof URI) {
            subject = fixUri((URI) subject);
        }
        predicate = fixUri(predicate);
        if (object instanceof URI) {
            object = fixUri((URI) object);
        } else if (object instanceof Literal) {
            object = fixNumber((Literal)object);
        }

        // No need to build a new statement if the old one matches.
        if (subject != statement.getSubject() || predicate != statement.getPredicate()
                || object != statement.getObject()) {
            statement = new StatementImpl(subject, predicate, object);
        }
        super.handleStatement(statement);
    }

    /**
     * Fixes numeric literal by ensuring it is actually contains numeric data.
     * If not, it will be converted to 0.
     * @param value
     * @return
     */
    private Value fixNumber(Literal value) {
        if (value.getDatatype().equals(XMLSchema.DECIMAL)) {
            if (!NumberUtils.isNumber(value.getLabel())) {
                return new LiteralImpl("0", XMLSchema.DECIMAL);
            }
        } else if (value.getDatatype().equals(XMLSchema.INTEGER)) {
            if (!NumberUtils.isNumber(value.getLabel())) {
                return new LiteralImpl("0", XMLSchema.INTEGER);
            }
        }
        return value;
    }

    /**
     * Fixes a uri if it contains something unacceptable otherwise just returns
     * the same uri.
     */
    private URI fixUri(URI r) {
        /*
         * Some dumps contained a versioned ontology but those are getting
         * unversioned soon.
         */
        if (r.stringValue().contains("ontology-0.0.1")) {
            r = new URIImpl(r.stringValue().replace("ontology-0.0.1", "ontology"));
        }
        if (r.stringValue().contains("ontology-beta")) {
            r = new URIImpl(r.stringValue().replace("ontology-beta", "ontology"));
        }
        if (r.stringValue().startsWith(Ontology.OLD_NAMESPACE)) {
            r = new URIImpl(r.stringValue().replace(Ontology.OLD_NAMESPACE, Ontology.NAMESPACE));
        }
        // Temporary bugfix for dump URLs having bad characters in them
        String fixed = StringUtils.replaceEach(r.stringValue(),
                new String[]{"\n", "|",   "\\",  "{",   "}",   "`",   "^"},
                new String[]{"",   "%7C", "%5C", "%7B", "%7D", "%60", "%5E"});
        if (!fixed.equals(r.stringValue())) {
            r = new URIImpl(fixed);
        }
        return r;
    }
}
