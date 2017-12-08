package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static org.wikidata.query.rdf.test.Matchers.binds;
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
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

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tests the munger that loads dumps.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
@RunWith(RandomizedRunner.class)
public class MungeIntegrationTest extends RandomizedTest {
    private static final Logger log = LoggerFactory.getLogger(MungeIntegrationTest.class);

    /**
     * Wikibase uris to test with.
     */
    private final WikibaseUris uris = new WikibaseUris("test.wikidata.org");


    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq");

    /**
     * Loads a truncated version of a test dump from test wikidata.
     */
    @Test
    @SuppressWarnings("checkstyle:illegalcatch")
    public void loadTest() throws IOException, InterruptedException, ExecutionException, ParseException, QueryEvaluationException {
        Reader from = utf8(getResource(MungeIntegrationTest.class, "test.ttl").openStream());
        PipedInputStream toHttp = new PipedInputStream();
        Writer writer = utf8(new PipedOutputStream(toHttp));
        OutputPicker<Writer> to = new AlwaysOutputPicker<>(writer);
        BlockingQueue<InputStream> queue = new ArrayBlockingQueue<>(1);
        queue.put(toHttp);
        Munge.Httpd httpd = new Httpd(10999, queue);
        Munger munger = new Munger(uris).singleLabelMode("en");
        ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Munge-IT-%d").build());
        Future<?> f = executor.submit(new Munge(uris, munger, from, to));
        httpd.start();
        try {
            assertEquals(944, rdfRepository.loadUrl("http://localhost:10999"));
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
        assertTrue(rdfRepository.ask(
                RDFS.prefix(uris.prefixes(new StringBuilder()))
                        .append("ASK { wd:Q10 rdfs:label \"Wikidata\"@en }").toString()));
        assertTrue(rdfRepository.ask(
                SchemaDotOrg.prefix(Ontology.prefix(new StringBuilder()))
                        .append("ASK { ontology:Dump schema:dateModified \"2015-04-02T10:54:56Z\"^^xsd:dateTime }")
                        .toString()));

        assertEquals(WikibaseRepository.inputDateFormat().parse("2015-04-02T10:54:56Z"), rdfRepository
                .fetchLeftOffTime());

        assertTrue(rdfRepository.ask(
                SchemaDotOrg.prefix(uris.prefixes(new StringBuilder()))
                        .append("ASK { wd:Q20 schema:dateModified ?date }").toString()));

        assertTrue(rdfRepository.ask(
                SchemaDotOrg.prefix(uris.prefixes(new StringBuilder()))
                        .append("ASK { wd:Q21 schema:version ?v }").toString()));

        TupleQueryResult results = rdfRepository.query(SchemaDotOrg.prefix(uris.prefixes(new StringBuilder()))
                .append("SELECT ?x WHERE { wd:Q14 wdt:P69 ?x }").toString());
        assertTrue(results.hasNext());
        BindingSet result = results.next();
        assertThat(result, binds("x", new LiteralImpl("1.23456789012345678901234567890123456789", XMLSchema.DECIMAL)));

    }
}
