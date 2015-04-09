package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

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
        super.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        Resource subject = statement.getSubject();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();
        /*
         * Some dumps contained a versioned ontology but those are getting
         * unversioned soon.
         */
        if (subject.stringValue().contains("ontology-0.0.1")) {
            subject = new URIImpl(subject.stringValue().replace("ontology-0.0.1", "ontology"));
        }
        if (predicate.stringValue().contains("ontology-0.0.1")) {
            predicate = new URIImpl(predicate.stringValue().replace("ontology-0.0.1", "ontology"));
        }
        if (object.stringValue().contains("ontology-0.0.1")) {
            object = new URIImpl(object.stringValue().replace("ontology-0.0.1", "ontology"));
        }
        if (subject != statement.getSubject() || predicate != statement.getPredicate()
                || object != statement.getObject()) {
            statement = new StatementImpl(subject, predicate, object);
        }
        super.handleStatement(statement);
    }
}
