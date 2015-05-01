package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static org.wikidata.query.rdf.tool.StreamUtils.utf8;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.text.ParseException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.Munge.AlwaysOutputPicker;
import org.wikidata.query.rdf.tool.Munge.Httpd;
import org.wikidata.query.rdf.tool.Munge.OutputPicker;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

/**
 * Tests the munger that loads dumps.
 */
public class MungeIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {
    private static final Logger log = LoggerFactory.getLogger(MungeIntegrationTest.class);

    public MungeIntegrationTest() {
        super(WikibaseUris.TEST_WIKIDATA);
    }

    /**
     * Loads a truncated version of a test dump from test wikidata.
     */
    @Test
    @SuppressWarnings("checkstyle:illegalcatch")
    public void loadTest() throws IOException, InterruptedException, ExecutionException, ParseException {
        Reader from = utf8(getResource(MungeIntegrationTest.class, "test.ttl").openStream());
        PipedInputStream toHttp = new PipedInputStream();
        Writer writer = utf8(new PipedOutputStream(toHttp));
        OutputPicker<Writer> to = new AlwaysOutputPicker<>(writer);
        BlockingQueue<InputStream> queue = new ArrayBlockingQueue<>(1);
        queue.put(toHttp);
        Munge.Httpd httpd = new Httpd(10999, queue);
        Munger munger = new Munger(uris()).singleLabelMode("en");
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> f = executor.submit(new Munge(uris(), munger, from, to));
        httpd.start();
        try {
            assertEquals(938, rdfRepository().loadUrl("http://localhost:10999"));
        } finally {
            try {
                /*
                 * Inside the finally block is like the one place where you have
                 * to catch exceptions so you don't eat the original exception.
                 */
                f.get();
                httpd.stop();
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("error shutting down", e);
            }
        }
        assertTrue(rdfRepository().ask(
                RDFS.prefixes(uris().prefixes(new StringBuilder()))
                        .append("ASK { wd:Q10 rdfs:label \"Wikidata\"@en }").toString()));
        assertTrue(rdfRepository().ask(
                SchemaDotOrg.prefix(Ontology.prefix(new StringBuilder()))
                        .append("ASK { ontology:Dump schema:dateModified \"2015-04-02T10:54:56Z\"^^xsd:dateTime }")
                        .toString()));

        assertEquals(WikibaseRepository.inputDateFormat().parse("2015-04-02T10:54:56Z"), rdfRepository()
                .fetchLeftOffTime());
    }
}
