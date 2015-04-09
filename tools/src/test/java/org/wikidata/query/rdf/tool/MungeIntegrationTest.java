package org.wikidata.query.rdf.tool;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
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
    public void loadTest() throws IOException, InterruptedException, ExecutionException {
        Reader from = new InputStreamReader(getResource(MungeIntegrationTest.class, "test.ttl").openStream(), UTF_8);
        PipedInputStream toHttp = new PipedInputStream();
        Writer writer = new OutputStreamWriter(new PipedOutputStream(toHttp), UTF_8);
        OutputPicker<Writer> to = new AlwaysOutputPicker<>(writer);
        BlockingQueue<InputStream> queue = new ArrayBlockingQueue<>(1);
        queue.put(toHttp);
        Munge.Httpd httpd = new Httpd(10999, queue);
        Munger munger = new Munger(uris).singleLabelMode("en");
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> f = executor.submit(new Munge(uris, munger, from, to));
        httpd.start();
        try {
            assertEquals(941, rdfRepository.loadUrl("http://localhost:10999"));
        } finally {
            try {
                f.get();
                httpd.stop();
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("error shutting down", e);
            }
        }
        assertTrue(rdfRepository.ask(RDFS.prefixes(uris.prefixes(new StringBuilder()))
                .append("ASK { entity:Q10 rdfs:label \"Wikidata\"@en }").toString()));
        assertTrue(rdfRepository.ask(SchemaDotOrg.prefix(Ontology.prefix(new StringBuilder()))
                .append("ASK { ontology:Dump schema:dateModified \"2015-04-02T10:54:56Z\"^^xsd:dateTime }").toString()));
    }
}
