package org.wikidata.query.rdf.tool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.CliUtils.BasicOptions;
import org.wikidata.query.rdf.tool.CliUtils.MungerOptions;
import org.wikidata.query.rdf.tool.CliUtils.WikibaseOptions;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.NormalizingRdfHandler;
import org.wikidata.query.rdf.tool.rdf.PrefixRecordingRdfHandler;

import com.codahale.metrics.Meter;
import com.google.common.base.Charsets;
import com.lexicalscope.jewel.cli.Option;

import fi.iki.elonen.NanoHTTPD;

/**
 * Munges a Wikidata RDF dump so that it can be loaded in a single import.
 */
public class Munge implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Munge.class);

    /**
     * CLI options for use with JewelCli.
     */
    public static interface Options extends BasicOptions, MungerOptions, WikibaseOptions {
        @Option(shortName = "f", defaultValue = "-", description = "Source file (or uri) to munge. Default is - aka stdin.")
        String from();

        @Option(shortName = "t", defaultValue = "-", description = "Destination of munge. Use port:<port_number> to start an "
                + "http server on that port. Default is - aka stdout. If the file's parent directories don't exist then they "
                + "will be created ala mkdir -p.")
        String to();

        @Option(defaultValue = "0", description = "Chunk size in entities. If specified then the \"to\" option must be a java "
                + "format string containing a single format identifier which is replaced with the chunk number port:<port_numer>. "
                + "%08d.ttl is a pretty good choice for format string. If \"to\" is in port form then every http request will "
                + "get the next chunk. Must be greater than 0 and less than " + Integer.MAX_VALUE + ".")
        int chunkSize();
    }

    public static void main(String[] args) {
        Options options = CliUtils.handleOptions(Options.class, args);
        WikibaseUris uris = new WikibaseUris(options.wikibaseHost());
        Munger munger = CliUtils.mungerFromOptions(options);

        int port = 0;
        if (options.to().startsWith("port:")) {
            port = Integer.parseInt(options.to().substring("port:".length()));
        }

        Reader from;
        try {
            from = CliUtils.reader(options.from());
        } catch (IOException e) {
            log.error("Error finding input", e);
            System.exit(1);
            return;
        }

        OutputPicker<Writer> to;
        Httpd httpd = null;
        try {
            if (options.chunkSize() > 0) {
                if (port > 0) {
                    // We have two slots just in case
                    BlockingQueue<InputStream> queue = new ArrayBlockingQueue<>(2);
                    httpd = new Httpd(port, queue);
                    to = new ChunkedPipedWriterOutputPicker(queue, options.chunkSize());
                } else {
                    to = new ChunkedFileWriterOutputPicker(options.to(), options.chunkSize());
                }
            } else {
                if (port > 0) {
                    PipedInputStream toHttp = new PipedInputStream();
                    Writer writer = new OutputStreamWriter(new PipedOutputStream(toHttp), Charsets.UTF_8);
                    BlockingQueue<InputStream> queue = new ArrayBlockingQueue<>(1);
                    queue.put(toHttp);
                    httpd = new Httpd(port, queue);
                    to = new AlwaysOutputPicker<>(writer);
                } else {
                    to = new AlwaysOutputPicker<>(CliUtils.writer(options.to()));
                }
            }
            if (httpd != null) {
                log.info("Starting embedded http sever on port {}", port);
                log.info("This process will exit when the whole dump has been served");
                httpd.start();
            }
        } catch (IOException e) {
            log.error("Error finding output", e);
            System.exit(1);
            return;
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting on httpd", e);
            System.exit(1);
            return;
        }
        try {
            Munge munge = new Munge(uris, munger, from, to);
            munge.run();
        } catch (RuntimeException e) {
            log.error("Fatal error munging RDF", e);
            System.exit(1);
        }
        if (httpd != null) {
            log.info("Finished munging and waiting for the http server to finish sending them");
            while (httpd.busy.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.info("Interrupted while waiting for http server to finish sending", e);
                    System.exit(1);
                }
            }
        }
    }

    private final WikibaseUris uris;
    private final Munger munger;
    private final Reader from;
    private final OutputPicker<Writer> to;

    public Munge(WikibaseUris uris, Munger munger, Reader from, OutputPicker<Writer> to) {
        this.uris = uris;
        this.munger = munger;
        this.from = from;
        this.to = to;
    }

    @Override
    public void run() {
        try {
            // TODO this is a temporary hack
            // RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            RDFParser parser = new ForbiddenOk.HackedTurtleParser();
            OutputPicker<RDFHandler> writer = new WriterToRDFWriterChunkPicker(to);
            EntityMungingRdfHandler handler = new EntityMungingRdfHandler(uris, munger, writer);
            parser.setRDFHandler(new NormalizingRdfHandler(handler));
            try {
                parser.parse(from, uris.entity());
            } catch (RDFParseException | RDFHandlerException | IOException e) {
                throw new RuntimeException(e);
            }
        } finally {
            try {
                from.close();
            } catch (IOException e) {
                log.error("Error closing input", e);
            }
            try {
                to.output().close();
            } catch (IOException e) {
                log.error("Error closing output", e);
            }
        }
    }

    /**
     * Collects statements about entities until it hits the next entity or the
     * end of the file, munges those statements, and then passes them to the
     * next handler. Note that this relies on the order of the data in the file
     * to be like:
     * <ul>
     * <li>http://www.wikidata.org/wiki/Special:EntityData/EntityId ?p ?o .
     * <li>everything about EntityId
     * <li>http://www.wikidata.org/wiki/Special:EntityData/NextEntityId ?p ?o .
     * <li>etc
     * </ul>
     * This is how the files are built so that is OK.
     */
    private static class EntityMungingRdfHandler implements RDFHandler {
        private final WikibaseUris uris;
        private final Munger munger;
        private final OutputPicker<RDFHandler> next;
        private final List<Statement> statements = new ArrayList<>();
        private final Meter entitiesMeter = new Meter();
        private boolean haveNonEntityDataStatements;
        private String entityId;

        public EntityMungingRdfHandler(WikibaseUris uris, Munger munger, OutputPicker<RDFHandler> next) {
            this.uris = uris;
            this.munger = munger;
            this.next = next;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            haveNonEntityDataStatements = false;
            next.output().startRDF();
        }

        @Override
        public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
            // Namespaces go through to the next handler.
            next.output().handleNamespace(prefix, uri);
        }

        @Override
        public void handleComment(String comment) throws RDFHandlerException {
            // Comments go right through to the next handler.
            next.output().handleComment(comment);
        }

        @Override
        public void handleStatement(Statement statement) throws RDFHandlerException {
            String subject = statement.getSubject().stringValue();
            if (subject.startsWith(uris.entityData())) {
                if (haveNonEntityDataStatements) {
                    munge();
                }
                if (statement.getPredicate().stringValue().equals(SchemaDotOrg.ABOUT)) {
                    entityId = statement.getObject().stringValue();
                    entityId = entityId.substring(entityId.lastIndexOf('/') + 1);
                }
                statements.add(statement);
                return;
            }
            if (subject.equals(Ontology.DUMP)) {
                /*
                 * Just pipe dump statements strait through.
                 */
                next.output().handleStatement(statement);
                return;
            }
            haveNonEntityDataStatements = true;
            statements.add(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            munge();
            next.output().endRDF();
        }

        private void munge() throws RDFHandlerException {
            try {
                log.debug("Munging {}", entityId);
                munger.munge(entityId, statements);
                for (Statement statement : statements) {
                    next.output().handleStatement(statement);
                }
                entitiesMeter.mark();
                if (entitiesMeter.getCount() % 10000 == 0) {
                    log.info("Processed {} entities at ({}, {}, {})", entitiesMeter.getCount(),
                            (long) entitiesMeter.getOneMinuteRate(), (long) entitiesMeter.getFiveMinuteRate(),
                            (long) entitiesMeter.getFifteenMinuteRate());
                }
                next.entitiesMunged((int) entitiesMeter.getCount());

            } catch (ContainedException e) {
                log.warn("Error munging {}", entityId, e);
            }
            statements.clear();
            haveNonEntityDataStatements = false;
        }
    }

    /**
     * Very simple HTTP server that only knows how to spit out results from a
     * queue.
     */
    public static class Httpd extends NanoHTTPD {
        /**
         * Flag that the server is still busy. We try to make sure to set this
         * to false if we're not busy so the process can exit.
         */
        private final AtomicBoolean busy = new AtomicBoolean(false);
        private final BlockingQueue<InputStream> results;

        public Httpd(int port, BlockingQueue<InputStream> results) {
            super(port);
            this.results = results;
        }

        @Override
        public Response serve(IHTTPSession session) {
            try {
                busy.set(true);
                Response response = new Response(Response.Status.OK, " application/x-turtle", results.take()) {
                    @Override
                    protected void send(OutputStream outputStream) {
                        super.send(outputStream);
                        busy.set(false);
                    }
                };
                response.setChunkedTransfer(true);
                return response;
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for a result", e);
                Thread.currentThread().interrupt();
                busy.set(false);
                return new Response(Response.Status.INTERNAL_ERROR, "text/plain", "internal server error");
            }
        }
    }

    /**
     * Picks the right RDFHandler for writing.
     */
    public interface OutputPicker<T> {
        /**
         * Get the handler to write to.
         */
        T output();

        /**
         * Update the number of entities already handled.
         */
        void entitiesMunged(int entitiesMunged);
    }

    /**
     * An output picker that always returns one output.
     */
    public static class AlwaysOutputPicker<T> implements OutputPicker<T> {
        private final T next;

        public AlwaysOutputPicker(T next) {
            this.next = next;
        }

        @Override
        public T output() {
            return next;
        }

        @Override
        public void entitiesMunged(int entitiesMunged) {
            // Intentionally do nothing
        }
    }

    /**
     * Output picker that starts new chunks after processing so many entities.
     */
    private static abstract class ChunkedWriterOutputPicker implements OutputPicker<Writer> {
        private final int chunkSize;
        private Writer writer;
        private int lastChunk = 1;

        public ChunkedWriterOutputPicker(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        @Override
        public Writer output() {
            if (writer == null) {
                writer = buildWriter(lastChunk);
            }
            return writer;
        }

        @Override
        public void entitiesMunged(int entitiesMunged) {
            int currentChunk = entitiesMunged / chunkSize + 1;
            if (lastChunk != currentChunk) {
                lastChunk = currentChunk;
                writer = buildWriter(lastChunk);
            }
        }

        protected abstract Writer buildWriter(long chunk);
    }

    public static class ChunkedFileWriterOutputPicker extends ChunkedWriterOutputPicker {
        private final String pattern;

        public ChunkedFileWriterOutputPicker(String pattern, int chunkSize) {
            super(chunkSize);
            this.pattern = pattern;
        }

        @Override
        protected Writer buildWriter(long chunk) {
            String file = String.format(Locale.ROOT, pattern, chunk);
            log.info("Switching to {}", file);
            try {
                return CliUtils.writer(file);
            } catch (IOException e) {
                throw new RuntimeException("Error switching chunks", e);
            }
        }
    }

    public static class ChunkedPipedWriterOutputPicker extends ChunkedWriterOutputPicker {
        private final BlockingQueue<InputStream> queue;

        public ChunkedPipedWriterOutputPicker(BlockingQueue<InputStream> queue, int chunkSize) {
            super(chunkSize);
            this.queue = queue;
        }

        @Override
        protected Writer buildWriter(long chunk) {
            PipedInputStream toQueue = new PipedInputStream();
            try {
                queue.put(toQueue);
                return new OutputStreamWriter(new PipedOutputStream(toQueue), Charsets.UTF_8);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Error switching chunks", e);
            }
        }
    }

    private static class WriterToRDFWriterChunkPicker implements OutputPicker<RDFHandler> {
        private final Map<String, String> prefixes = new LinkedHashMap<String, String>();
        private final OutputPicker<Writer> next;
        private Writer lastWriter;
        private RDFHandler handler;

        public WriterToRDFWriterChunkPicker(OutputPicker<Writer> next) {
            this.next = next;
            lastWriter = next.output();
            try {
                setHandlerFromLastWriter();
            } catch (RDFHandlerException e) {
                throw new RuntimeException("Error setting up first rdf writer", e);
            }
        }

        @Override
        public RDFHandler output() {
            Writer nextWriter = next.output();
            if (nextWriter == lastWriter) {
                return handler;
            }
            try {
                /*
                 * When we hit a new chunk we have to terminate rdf and start it
                 * on the next chunk.
                 */
                handler.endRDF();
                lastWriter.close();
                lastWriter = nextWriter;
                setHandlerFromLastWriter();
                handler.startRDF();
            } catch (RDFHandlerException | IOException e) {
                throw new RuntimeException("Error switching chunks", e);
            }
            return handler;
        }

        @Override
        public void entitiesMunged(int entitiesMunged) {
            next.entitiesMunged(entitiesMunged);
        }

        private void setHandlerFromLastWriter() throws RDFHandlerException {
            handler = Rio.createWriter(RDFFormat.TURTLE, lastWriter);
            handler = new PrefixRecordingRdfHandler(handler, prefixes);
            for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
                handler.handleNamespace(prefix.getKey(), prefix.getValue());
            }
        }
    }

    /**
     * We need access to getMessage from exceptions. This is brittle but
     * (hopefully) temporary.
     */
    private static class ForbiddenOk {
        private static class HackedTurtleParser extends TurtleParser {
            @Override
            protected URI parseURI() throws IOException, RDFParseException {
                try {
                    return super.parseURI();
                } catch (RDFParseException e) {
                    if (e.getMessage().startsWith("IRI includes string escapes: ")
                            || e.getMessage().startsWith("IRI included an unencoded space: '32'")) {
                        log.warn("Attempting to recover from", e);
                        // Unless we hit a \> then consume until we get to a >
                        if (!e.getMessage().startsWith("IRI includes string escapes: '\\62'")) {
                            while (readCodePoint() != '>') {
                            }
                        }
                        return super.resolveURI("http://example.com/error");
                    }
                    throw e;
                }
            }

            @Override
            protected void parseStatement() throws IOException, RDFParseException, RDFHandlerException {
                try {
                    super.parseStatement();
                } catch (RDFParseException e) {
                    if (e.getMessage().startsWith("Namespace prefix 'Warning' used but not defined")) {
                        log.warn("Attempting to recover from", e);
                        /*
                         * Just dump the rest of the line. Hopefully that'll be
                         * enough to recover.
                         */
                        while (readCodePoint() != '\n') {
                        }
                    }
                }
            }
        }
    }
}
