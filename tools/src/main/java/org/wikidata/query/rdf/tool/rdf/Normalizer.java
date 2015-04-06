package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * An RDFHandler that wraps another handler normalizing any of the (currently)
 * rather different wikidata output forms into a single form.
 */
public class Normalizer implements RDFHandler {
    private final RDFHandler next;

    public Normalizer(RDFHandler next) {
        this.next = next;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        next.startRDF();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        next.endRDF();
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        next.handleNamespace(prefix, uri);
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        next.handleComment(comment);
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        String subject = statement.getSubject().stringValue();
        /*
         * Some dumps contained a versioned ontology but those are getting
         * unversioned soon.
         */
        if (subject.contains("ontology-0.0.1")) {
            subject = subject.replace("ontology-0.0.1", "ontology");
        }
        if (!statement.getSubject().stringValue().equals(subject)) {
            statement = new StatementImpl(new URIImpl(subject), statement.getPredicate(), statement.getObject());
        }
        next.handleStatement(statement);
    }
}
