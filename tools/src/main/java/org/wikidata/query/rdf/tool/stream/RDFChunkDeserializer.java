package org.wikidata.query.rdf.tool.stream;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;

public class RDFChunkDeserializer {
    private final RDFParserSuppliers rdfParserSuppliers;

    public RDFChunkDeserializer(RDFParserSuppliers rdfParserSuppliers) {
        this.rdfParserSuppliers = rdfParserSuppliers;
    }

    public List<Statement> deser(RDFDataChunk chunk, String baseUri) {
        return deser(new StringReader(chunk.getData()), chunk.getMimeType(), baseUri);
    }

    public List<Statement> deser(Reader reader, String mime, String baseUri) {
        List<Statement> stmts = new ArrayList<>();
        StatementCollector collector = new StatementCollector(stmts);
        RDFParser parser = rdfParserSuppliers.forMimeType(mime).get(collector);
        try {
            parser.parse(reader, baseUri);
        } catch (IOException | RDFParseException | RDFHandlerException e) {
            throw new IllegalArgumentException("Cannot parse RDF data", e);
        }
        return stmts;
    }
}
