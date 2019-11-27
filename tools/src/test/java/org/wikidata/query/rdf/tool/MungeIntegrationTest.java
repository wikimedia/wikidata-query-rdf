package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.tool.StreamUtils.utf8;
import static org.wikidata.query.rdf.tool.rdf.RdfRepository.UpdateMode.NON_MERGING;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.time.Instant;

import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.google.common.io.Closer;

/**
 * Tests the munger that loads dumps.
 */
@SuppressWarnings("checkstyle:illegalcatch")
public class MungeIntegrationTest {
    /**
     * Wikibase uris to test with.
     */
    private final UrisScheme uris = UrisSchemeFactory.forHost("test.wikidata.org");

    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq", NON_MERGING);

    /**
     * RDF client.
     */
    private RdfClient rdfClient = rdfRepository.getClient();
    private Closer closer = Closer.create();

    /**
     * Loads a truncated version of a test dump from test wikidata.
     */
    @Test
    public void loadTest() throws IOException, ParseException, QueryEvaluationException {
        try {
            loadDumpIntoRepo("test.ttl", 944, closer);
        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }
        assertTrue(ask("ASK { wd:Q10 rdfs:label \"Wikidata\"@en }"));

        assertTrue(rdfClient.ask(
                Ontology.prefix(new StringBuilder())
                        .append("ASK { ontology:Dump schema:dateModified \"2015-04-02T10:54:56Z\"^^xsd:dateTime }")
                        .toString()));

        assertEquals(WikibaseRepository.INPUT_DATE_FORMATTER.parse("2015-04-02T10:54:56Z", Instant::from),
                rdfRepository.fetchLeftOffTime());

        assertTrue(ask("ASK { wd:Q20 schema:dateModified ?date }"));
        assertTrue(ask("ASK { wd:Q21 schema:version ?v }"));

        TupleQueryResult results = rdfClient.query(uris.prefixes(new StringBuilder())
                .append("SELECT ?x WHERE { wd:Q14 wdt:P69 ?x }").toString());
        assertTrue(results.hasNext());
        assertThat(results.next(), binds("x", new LiteralImpl("1.23456789012345678901234567890123456789", XMLSchema.DECIMAL)));
    }

    private void loadDumpIntoRepo(String dumpName, int count, Closer closer) throws Exception {
        Reader from = utf8(getResource(MungeIntegrationTest.class, dumpName).openStream());
        closer.register(from);
        Munger munger = Munger.builder(uris).singleLabelMode("en").build();
        File file = File.createTempFile("munge-test", ".ttl");
        String fileURL = file.toURI().toURL().toString();
        Munge munge = new Munge(uris, munger, from, Integer.MAX_VALUE, file.getAbsolutePath());
        munge.run();
        assertEquals(count, (long) rdfRepository.getClient().loadUrl(fileURL));
    }

    /**
     * Run ASK query with prefixes.
     */
    private boolean ask(String query) {
        return rdfClient.ask(uris.prefixes(new StringBuilder()).append(query).toString());
    }

    /**
     * Loads a lexeme dump.
     */
    @Test
    public void lexemeTest() throws IOException, QueryEvaluationException {
        try {
            loadDumpIntoRepo("lexeme.ttl", 38, closer);
        } catch (Throwable t) {
            throw closer.rethrow(t);
        } finally {
            closer.close();
        }

        // Metadata
        assertTrue(ask("ASK { wd:L2 schema:dateModified ?date }"));
        assertTrue(ask("ASK { wd:L3 schema:version ?v }"));
        assertTrue(ask("ASK { wd:L3 a ontolex:LexicalEntry }"));
        assertFalse(ask("ASK { wd:L3 a wikibase:Lexeme }"));
        // lemma
        assertTrue(ask("ASK { wd:L2 wikibase:lemma \"duck\"@en }"));
        // another lemma
        assertTrue(ask("ASK { wd:L2 wikibase:lemma \"quack\"@en-gb }"));
        // but no label
        assertFalse(ask("ASK { wd:L2 rdfs:label ?l }"));
        // language
        assertTrue(ask("ASK { wd:L2 dct:language wd:Q6 }"));
        // TODO: schema:inLanguage?
        // lexical cat
        assertTrue(ask("ASK { wd:L2 wikibase:lexicalCategory wd:Q7 }"));
        // forms
        assertTrue(ask("ASK { wd:L2 ontolex:lexicalForm wd:L2-F1 }"));
        assertTrue(ask("ASK { wd:L2 ontolex:lexicalForm wd:L2-F2 }"));
        assertTrue(ask("ASK { wd:L2-F1 a ontolex:Form }"));
        assertFalse(ask("ASK { wd:L3 a wikibase:Form }"));
        // form representation
        assertTrue(ask("ASK { wd:L2-F1 ontolex:representation \"duck\"@en }"));
        // but no label
        assertFalse(ask("ASK { wd:L2-F1 rdfs:label \"duck\"@en }"));
        // form feature
        assertTrue(ask("ASK { wd:L2-F1 wikibase:grammaticalFeature wd:Q3 }"));
        // form statement
        TupleQueryResult results = rdfClient.query(uris.prefixes(new StringBuilder())
                .append("SELECT ?x WHERE { wd:L2 ontolex:lexicalForm/wdt:P7 ?x }").toString());
        assertTrue(results.hasNext());
        assertThat(results.next(), binds("x", new URIImpl(uris.entityIdToURI("Q3"))));
        // TODO: senses
    }
}
