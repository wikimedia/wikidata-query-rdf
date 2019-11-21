package org.wikidata.query.rdf.tool;

import static java.lang.Boolean.FALSE;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.mungerFromOptions;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.openrdf.rio.turtle.TurtleParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.options.MungeOptions;
import org.wikidata.query.rdf.tool.options.OptionsUtils;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.NormalizingRdfHandler;
import org.wikidata.query.rdf.tool.rdf.PrefixRecordingRdfHandler;

import com.codahale.metrics.Meter;

import de.thetaphi.forbiddenapis.SuppressForbidden;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Munges a Wikidata RDF dump so that it can be loaded in a single import.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class Munge {
    private static final Logger log = LoggerFactory.getLogger(Munge.class);

    /**
     * Run a bulk munge configured from the command line.
     */
    @SuppressWarnings("checkstyle:illegalcatch")
    public static void main(String[] args) {
        MungeOptions options = handleOptions(MungeOptions.class, args);
        UrisScheme uris = OptionsUtils.WikibaseOptions.wikibaseUris(options);
        Munger munger = mungerFromOptions(options);

        OutputPicker<Writer> to;
        try {
            if (options.chunkSize() > 0) {
                to = new ChunkedFileWriterOutputPicker(options.to(), options.chunkSize());
            } else {
                to = new AlwaysOutputPicker<>(CliUtils.writer(options.to()));
            }
        } catch (IOException e) {
            log.error("Error finding output", e);
            System.exit(1);
            return;
        }
        try {
            Munge munge = new Munge(uris, munger, CliUtils.reader(options.from()), to);
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
    /**
     * Where the munged RDF is synced.
     */
    private final OutputPicker<Writer> to;

    public Munge(UrisScheme uris, Munger munger, Reader from, OutputPicker<Writer> to) {
        this.uris = uris;
        this.munger = munger;
        this.from = from;
        this.to = to;
    }

    public void run() throws Exception {
        try {
            // TODO this is a temporary hack
            // RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            RDFParser parser = new ForbiddenOk.HackedTurtleParser();
            OutputPicker<RDFHandler> writer = new WriterToRDFWriterChunkPicker(to);
            EntityMungingRdfHandler handler = new EntityMungingRdfHandler(uris, munger, writer);
            parser.setRDFHandler(new NormalizingRdfHandler(handler));
            parser.parse(from, uris.root());
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
    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "the unread lastStatement field is used for debugging")
    private static class EntityMungingRdfHandler implements RDFHandler {
        /**
         * Uris for this instance of wikibase. We match on these.
         */
        private final UrisScheme uris;
        /**
         * Actually munges the entities!
         */
        private final Munger munger;
        /**
         * The place where we sync munged entities.
         */
        private final OutputPicker<RDFHandler> next;
        /**
         * The statements about the current entity.
         */
        private final List<Statement> statements = new ArrayList<>();
        /**
         * Meter measuring the number of entities we munge in grand load average
         * style.
         */
        private final Meter entitiesMeter = new Meter();
        /**
         * Have we hit any non Special:EntityData statements? Used to make sure
         * we properly pick up the first few statements in every entity.
         */
        private boolean haveNonEntityDataStatements;
        /**
         * The current entity being read. When we hit a new entity we start send
         * the old statements to the munger and then sync them to next.
         */
        private String entityId;

        EntityMungingRdfHandler(UrisScheme uris, Munger munger, OutputPicker<RDFHandler> next) {
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
        @SuppressFBWarnings(value = "STT_STRING_PARSING_A_FIELD", justification = "low priority to fix")
        public void handleStatement(Statement statement) throws RDFHandlerException {
            String subject = statement.getSubject().stringValue();
            if (subject.startsWith(uris.entityDataHttps()) || subject.startsWith(uris.entityData())) {
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
                if (statement.getPredicate().stringValue().equals(SchemaDotOrg.SOFTWARE_VERSION)) {
                    munger.setFormatVersion(statement.getObject().stringValue());
                }
                /*
                 * Just pipe dump statements strait through.
                 */
                next.output().handleStatement(statement);
                return;
            }
            if (statement.getPredicate().stringValue().equals(OWL.SAME_AS)) {
                // Temporary fix for T100463
                if (haveNonEntityDataStatements) {
                    munge();
                }
                entityId = subject.substring(subject.lastIndexOf('/') + 1);
                statements.add(statement);
                haveNonEntityDataStatements = true;
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

        /**
         * Munge an entity's worth of RDF and then sync it the the output.
         *
         * @throws RDFHandlerException if there is an error syncing it
         */
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
        /**
         * The output to return.
         */
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
    private abstract static class ChunkedWriterOutputPicker implements OutputPicker<Writer> {
        /**
         * The number of entities per writer.
         */
        private final int chunkSize;
        /**
         * Writer returned by output(). Initialized on first call to output.
         */
        private Writer writer;
        /**
         * The chunk number that writer was built for.
         */
        private int lastChunk = 1;

        ChunkedWriterOutputPicker(int chunkSize) {
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

        /**
         * Build the next writer.
         */
        protected abstract Writer buildWriter(long chunk);
    }

    /**
     * OutputPicker that writes to files.
     */
    public static class ChunkedFileWriterOutputPicker extends ChunkedWriterOutputPicker {
        /**
         * Pattern for file names.
         */
        private final String pattern;

        public ChunkedFileWriterOutputPicker(String pattern, int chunkSize) {
            super(chunkSize);
            this.pattern = pattern;
        }

        @Override
        @SuppressFBWarnings(
                value = "EXS_EXCEPTION_SOFTENING_NO_CHECKED",
                justification = "Hiding IOException is suspicious, but seems to be the usual pattern in this project")
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

    /**
     * Adapts an OutputPicker for writers to one for RDFHandlers, taking care to
     * always add all the prefixes.
     */
    private static class WriterToRDFWriterChunkPicker implements OutputPicker<RDFHandler> {
        /**
         * Map containing prefixes that have been written to any RDFHandler that
         * we then write to all the next handlers.
         */
        private final Map<String, String> prefixes = new LinkedHashMap<>();
        /**
         * The output picker for the writers.
         */
        private final OutputPicker<Writer> next;
        /**
         * The lastWriter used to build the RDFHandler. If it changes we build a
         * new RDFHandler.
         */
        private Writer lastWriter;
        /**
         * The current RDFHandler to write to.
         */
        private RDFHandler handler;

        WriterToRDFWriterChunkPicker(OutputPicker<Writer> next) {
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

        /**
         * Set the next handler from the lastWriter field.
         *
         * @throws RDFHandlerException if the handler throws it while
         *             initializing
         */
        private void setHandlerFromLastWriter() throws RDFHandlerException {
            final RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, lastWriter);
            final WriterConfig config = writer.getWriterConfig();
            config.set(BasicWriterSettings.PRETTY_PRINT, FALSE);
            handler = new PrefixRecordingRdfHandler(writer, prefixes);
            for (Map.Entry<String, String> prefix : prefixes.entrySet()) {
                handler.handleNamespace(prefix.getKey(), prefix.getValue());
            }
        }
    }

    /**
     * We need access to getMessage from exceptions. This is brittle but
     * (hopefully) temporary.
     */
    @SuppressForbidden
    private static class ForbiddenOk {
        /**
         * TurtleParser that tries to recover from errors we see in wikibase.
         */
        private static class HackedTurtleParser extends TurtleParser {
            @Override
            protected URI parseURI() throws IOException, RDFParseException {
                try {
                    return super.parseURI();
                } catch (RDFParseException e) {
                    if (e.getMessage().startsWith("IRI includes string escapes: ")
                            || e.getMessage().startsWith("IRI included an unencoded space: '32'")) {
                        log.warn("Attempting to recover from", e);
                        if (!e.getMessage().startsWith("IRI includes string escapes: '\\62'")) {
                            while (readCodePoint() != '>') {
                                /*
                                 * Dump until the end of the uri.
                                 */
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
                        while (readCodePoint() != '\n') {
                            /*
                             * Just dump the rest of the line. Hopefully that'll
                             * be enough to recover.
                             */
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
    }
}
