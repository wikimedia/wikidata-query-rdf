package org.wikidata.query.rdf.tool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
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
import org.wikidata.query.rdf.tool.rdf.Normalizer;

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
        @Option(shortName = "f", defaultValue = "-", description = "Source file (or uri) to munge.  Default is - aka stdin.")
        String from();

        @Option(shortName = "t", defaultValue = "-", description = "Destination of munge. Use port:<port_number> to start an "
                + "http server on that port. Default is - aka stdout.")
        String to();
    }

    public static void main(String[] args) {
        Options options = CliUtils.handleOptions(Options.class, args);
        WikibaseUris uris = new WikibaseUris(options.wikibaseHost());
        Munger munger = CliUtils.mungerFromOptions(options);

        if (options.to().startsWith("port:")) {
            int port = Integer.parseInt(options.to().substring("port:".length()));
            Httpd http = new Httpd(port, uris, munger, options.from());
            try {
                log.info("Starting embedded http server for munged rdf on port {}.", port);
                http.start();
                // Just wait until ctrl-c
                while (true) {
                    try {
                        Thread.sleep(100000000L);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            } catch (IOException e) {
                log.error("Error starting embedded http server", e);
                return;
            }
        }

        Writer to;
        try {
            to = new BufferedWriter(new OutputStreamWriter(CliUtils.outputStream(options.to()), Charsets.UTF_8));
        } catch (IOException e) {
            log.error("Error finding output", e);
            System.exit(1);
            return;
        }
        Reader from;
        try {
            from = CliUtils.reader(options.from());
        } catch (IOException e) {
            log.error("Error finding input", e);
            System.exit(1);
            return;
        }
        try {
            Munge munge = new Munge(uris, munger, from, to);
            munge.run();
        } catch (RuntimeException e) {
            log.error("Fatal error munging RDF", e);
        }
    }

    private final WikibaseUris uris;
    private final Munger munger;
    private final Reader from;
    private final Writer to;

    public Munge(WikibaseUris uris, Munger munger, Reader from, Writer to) {
        this.uris = uris;
        this.munger = munger;
        this.from = from;
        this.to = to;
    }

    @Override
    public void run() {
        try {
            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, to);
            RDFHandler handler = new EntityMungingRdfHandler(uris, munger, writer);
            handler = new Normalizer(handler);
            parser.setRDFHandler(handler);
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
                to.close();
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
        private final RDFHandler next;
        private final List<Statement> statements = new ArrayList<>();
        private boolean haveNonEntityDataStatements;
        private String entityId;

        public EntityMungingRdfHandler(WikibaseUris uris, Munger munger, RDFHandler next) {
            this.uris = uris;
            this.munger = munger;
            this.next = next;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            haveNonEntityDataStatements = false;
            next.startRDF();
        }

        @Override
        public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
            // Namespaces go through to the next handler.
            next.handleNamespace(prefix, uri);
        }

        @Override
        public void handleComment(String comment) throws RDFHandlerException {
            // Comments go right through to the next handler.
            next.handleComment(comment);
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
                next.handleStatement(statement);
                return;
            }
            haveNonEntityDataStatements = true;
            statements.add(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            munge();
            next.endRDF();
        }

        private void munge() throws RDFHandlerException {
            try {
                log.debug("Munging {}", entityId);
                munger.munge(entityId, statements);
                for (Statement statement : statements) {
                    next.handleStatement(statement);
                }
            } catch (ContainedException e) {
                log.warn("Error munging {}", entityId, e);
            }
            statements.clear();
            haveNonEntityDataStatements = false;
        }
    }

    /**
     * Very simple HTTP server that only knows how to spit out the result of the
     * munge. Useful because tools like Blazegraph can import from any URI so
     * this removes the need to create a temporary file.
     */
    public static class Httpd extends NanoHTTPD {
        private final WikibaseUris uris;
        private final Munger munger;
        private final String source;

        public Httpd(int port, WikibaseUris uris, Munger munger, String source) {
            super(port);
            this.uris = uris;
            this.munger = munger;
            this.source = source;
        }

        @Override
        public Response serve(IHTTPSession session) {
            try {
                PipedInputStream sentToResponse = new PipedInputStream();
                Reader from = new InputStreamReader(CliUtils.inputStream(source), Charsets.UTF_8);
                final Writer to = new OutputStreamWriter(new PipedOutputStream(sentToResponse), Charsets.UTF_8);
                final Munge munge = new Munge(uris, munger, from, to);
                Thread thread = new Thread(munge);
                thread.setUncaughtExceptionHandler(CliUtils.loggingUncaughtExceptionHandler("Error serving rdf", log));
                thread.start();
                Response response = new Response(Response.Status.OK, " application/x-turtle", sentToResponse);
                response.setChunkedTransfer(true);
                return response;
            } catch (IOException e) {
                log.error("Error serving rdf", e);
                return new Response(Response.Status.INTERNAL_ERROR, "text/plain", "internal server error");
            }
        }
    }
}
