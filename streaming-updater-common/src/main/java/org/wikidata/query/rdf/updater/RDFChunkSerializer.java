package org.wikidata.query.rdf.updater;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;

public class RDFChunkSerializer {
    private final RDFWriterRegistry writerRegistry;

    public RDFChunkSerializer(RDFWriterRegistry writerRegistry) {
        this.writerRegistry = writerRegistry;
    }

    public List<RDFDataChunk> serializeAsChunks(Collection<Statement> statements, String mime, int softMaxSize) {
        List<RDFDataChunk> chunks = new ArrayList<>();
        StringWriter stringWriter = new StringWriter();
        RDFFormat fileFormatForMimeType = writerRegistry.getFileFormatForMIMEType(mime);
        if (fileFormatForMimeType == null) {
            throw new IllegalArgumentException("Unsupported mime type [" + mime + "]");
        }
        RDFWriter writer = writerRegistry.get(fileFormatForMimeType).getWriter(stringWriter);
        try {
            boolean pending = false;
            writer.startRDF();
            for (Statement s : statements) {
                writer.handleStatement(s);
                pending = true;
                if (stringWriter.getBuffer().length() > softMaxSize) {
                    writer.endRDF();
                    chunks.add(new RDFDataChunk(stringWriter.toString(), mime));
                    stringWriter.getBuffer().setLength(0);
                    pending = false;
                    writer.startRDF();
                }
            }
            if (pending) {
                writer.endRDF();
                chunks.add(new RDFDataChunk(stringWriter.toString(), mime));
            }
        } catch (RDFHandlerException e) {
            throw new IllegalStateException("Cannot write RDF chunks", e);
        }
        return chunks;
    }
}
