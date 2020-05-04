package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;

public final class RDFParserSuppliers {
    private RDFParserSuppliers() {}

    /**
     * Parser for TURTLE which preserves bnode ids.
     */
    public static RDFParserSupplier defaultRdfParser() {
        return handler -> {
            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.getParserConfig().set(BasicParserSettings.PRESERVE_BNODE_IDS, Boolean.TRUE);
            parser.setRDFHandler(new NormalizingRdfHandler(handler));
            return parser;
        };
    }
}
