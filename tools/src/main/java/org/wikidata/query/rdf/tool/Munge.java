package org.wikidata.query.rdf.tool;

import static java.lang.Boolean.FALSE;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.options.MungeOptions;
import org.wikidata.query.rdf.tool.options.OptionsUtils;
import org.wikidata.query.rdf.tool.rdf.AsyncRDFHandler;
import org.wikidata.query.rdf.tool.rdf.EntityMungingRdfHandler;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.NormalizingRdfHandler;
import org.wikidata.query.rdf.tool.rdf.PrefixRecordingRdfHandler;

/**
 * Munges a Wikidata RDF dump so that it can be loaded in a single import.
 */
public class Munge {
    private static final Logger log = LoggerFactory.getLogger(Munge.class);
    public static final int BUFFER_SIZE = 20000;

    /**
     * Run a bulk munge configured from the command line.
     */
    @SuppressWarnings("IllegalCatch")
    public static void main(String[] args) {
        MungeOptions options = handleOptions(MungeOptions.class, args);
        UrisScheme uris = OptionsUtils.WikibaseOptions.wikibaseUris(options);
        Munger munger = mungerFromOptions(options);

        int chunksize = options.chunkSize();
        if (chunksize < 1) {
            chunksize = Integer.MAX_VALUE;
        }
        try {
            Munge munge = new Munge(uris, munger, CliUtils.reader(options.from()), chunksize, options.to());
            munge.run();
        } catch (Exception e) {
            log.error("Fatal error munging RDF", e);
            System.exit(1);
        }
    }

    /**
     * Uris for this wikibase instance. Used to match the rdf as its read.
     */
    private final UrisScheme uris;
    /**
     * Munges the rdf.
     */
    private final Munger munger;
    /**
     * Source of the rdf.
     */
    private final Reader from;

    private final int chunkSize;
    private final String chunkFileFormat;

    public Munge(UrisScheme uris, Munger munger, Reader from, int chunkSize, String chunkFileFormat) {
        this.uris = uris;
        this.munger = munger;
        this.from = from;
        this.chunkSize = chunkSize;
        this.chunkFileFormat = chunkFileFormat;
    }

    public void run() throws RDFHandlerException, IOException, RDFParseException, InterruptedException {
        try {
            AsyncRDFHandler chunkWriter = AsyncRDFHandler.processAsync(new RDFChunkWriter(chunkFileFormat), false, BUFFER_SIZE);
            AtomicLong actualChunk = new AtomicLong(0);
            EntityMungingRdfHandler.EntityCountListener chunker = (entities) -> {
                long currentChunk = entities / chunkSize;
                if (currentChunk != actualChunk.get()) {
                    actualChunk.set(currentChunk);
                    // endRDF will cause RDFChunkWriter to start writing a new chunk
                    chunkWriter.endRDF();
                }
            };
            EntityMungingRdfHandler munger = new EntityMungingRdfHandler(uris, this.munger, chunkWriter, chunker);
            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.setRDFHandler(AsyncRDFHandler.processAsync(new NormalizingRdfHandler(munger), true, BUFFER_SIZE));
            parser.parse(from, uris.root());
            // thread:main: parser -> AsyncRDFHandler -> queue
            // thread:replayer1: Normalizing/Munging -> AsyncRDFHandler -> queue
            // thread:replayer2: RDFChunkWriter -> RDFWriter -> IO
            chunkWriter.waitForCompletion();
        } finally {
            try {
                from.close();
            } catch (IOException e) {
                log.error("Error closing input", e);
            }
        }
    }

    /**
     * Allows to open a new file on the next RDF event after endRDF() is called.
     * It takes care of recording and replaying the namespaces.
     */
    private static class RDFChunkWriter implements RDFHandler {
        /**
         * Map containing prefixes that have been written to any RDFHandler that
         * we then write to all the next handlers.
         */
        private final Map<String, String> prefixes = new LinkedHashMap<>();

        /**
         * The file name pattern to use when writing to the next chunk.
         */
        private final String pattern;

        /**
         * The lastWriter used to build the RDFHandler. If it changes we build a
         * new RDFHandler.
         */
        private Writer currentWriter;

        private long chunk = 1;
        /**
         * The current RDFHandler to write to.
         */
        private RDFHandler handler;

        RDFChunkWriter(String pattern) throws RDFHandlerException {
            this.pattern = pattern;
        }

        private void maybeOpenNextChunk() throws RDFHandlerException {
            if (currentWriter != null) {
                return;
            }
            currentWriter = buildWriter(chunk++);
            final RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, currentWriter);
            final WriterConfig config = writer.getWriterConfig();
            config.set(BasicWriterSettings.PRETTY_PRINT, FALSE);
            handler = new PrefixRecordingRdfHandler(writer, prefixes);
            handler.startRDF();
            for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
                handler.handleNamespace(prefix.getKey(), prefix.getValue());
            }
        }

        private Writer buildWriter(long chunk) throws RDFHandlerException {
            String file = String.format(Locale.ROOT, pattern, chunk);
            log.info("Switching to {}", file);
            try {
                return CliUtils.writer(file);
            } catch (IOException e) {
                throw new RDFHandlerException("Error switching chunks", e);
            }
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            maybeOpenNextChunk();
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            handler.endRDF();
            try {
                currentWriter.close();
            } catch (IOException e) {
                throw new RDFHandlerException(e);
            }
            currentWriter = null;
            handler = null;
        }

        @Override
        public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
            maybeOpenNextChunk();
            handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {
            maybeOpenNextChunk();
            handler.handleStatement(st);
        }

        @Override
        public void handleComment(String comment) throws RDFHandlerException {
            maybeOpenNextChunk();
            handler.handleComment(comment);
        }

    }
}
