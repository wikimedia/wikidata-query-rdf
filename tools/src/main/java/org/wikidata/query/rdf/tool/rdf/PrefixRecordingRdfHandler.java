package org.wikidata.query.rdf.tool.rdf;

import java.util.Map;

import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * RDFHandler that delegates while recording prefixes.
 */
public class PrefixRecordingRdfHandler extends DelegatingRdfHandler {
    private final Map<String, String> prefixes;

    public PrefixRecordingRdfHandler(RDFHandler next, Map<String, String> prefixes) {
        super(next);
        this.prefixes = prefixes;
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        prefixes.put(prefix, uri);
        super.handleNamespace(prefix, uri);
    }
}
