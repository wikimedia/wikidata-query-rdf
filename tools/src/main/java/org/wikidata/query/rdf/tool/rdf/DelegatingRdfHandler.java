package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * RDFHandler that delegates all operations to a next handler. Extend this to
 * implement mostly pass-through handlers.
 */
public class DelegatingRdfHandler implements RDFHandler {
    /**
     * Handler to which to delegate all actions.
     */
    private final RDFHandler next;

    public DelegatingRdfHandler(RDFHandler next) {
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
        next.handleStatement(statement);
    }
}
