package org.wikidata.query.rdf.tool.rdf;

import java.io.Serializable;

import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFParser;

@FunctionalInterface
public interface RDFParserSupplier extends Serializable {
    RDFParser get(RDFHandler handle);
}
