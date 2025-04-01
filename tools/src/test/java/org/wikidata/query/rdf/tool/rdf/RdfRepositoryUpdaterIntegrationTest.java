package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statements;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClient;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClientRetryer;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyHost;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyPort;
import static org.wikidata.query.rdf.tool.RdfRepositoryForTesting.url;
import static org.wikidata.query.rdf.tool.Update.getRdfClientTimeout;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.StatementCollector;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.Munge;
import org.wikidata.query.rdf.tool.StreamUtils;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

public class RdfRepositoryUpdaterIntegrationTest {
    private final RdfClient client = new RdfClient(
            buildHttpClient(getHttpProxyHost(), getHttpProxyPort()), url("/namespace/wdq/sparql"),
            buildHttpClientRetryer(),
            getRdfClientTimeout(),
            RdfClient.DEFAULT_MAX_RESPONSE_SIZE
    );

    private final UrisScheme uris = UrisSchemeFactory.getURISystem();
    List<String> entityIdsToDelete = new ArrayList<>();
    Map<String, Collection<Statement>> entitiesToReconcile = new HashMap<>();

    @Test
    public void testSimplePatch() {
        // prepare the test dataset
        client.update("DELETE DATA {" +
                "<uri:b> <uri:b> <uri:b> ." +
                "<uri:shared-1> <uri:shared-1> <uri:shared-1> ." +
                "};\n" +
                "INSERT DATA { <uri:a> <uri:a> <uri:a> .\n" +
                "<uri:shared-0> <uri:shared-0> <uri:shared-0> . };\n");

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, uris);
        ConsumerPatch patch = new ConsumerPatch(statements("uri:b"), statements("uri:shared-0", "uri:shared-1"),
                statements("uri:a", "uri:x"), statements("uri:ignored"), entityIdsToDelete, entitiesToReconcile);
        Instant avgEventTime = Instant.EPOCH.plus(1, ChronoUnit.MINUTES);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        assertThat(rdfPatchResult.getActualMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getExpectedMutations()).isEqualTo(3);
        assertThat(rdfPatchResult.getActualSharedElementsMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getPossibleSharedElementMutations()).isEqualTo(3);
        assertThat(rdfPatchResult.getDeleteMutations()).isZero();
        assertThat(client.ask("ask {<uri:a> <uri:a> <uri:a>}")).isFalse();
        assertThat(client.ask("ask {<uri:b> <uri:b> <uri:b>}")).isTrue();
        assertThat(client.ask("ask {<uri:shared-0> <uri:shared-0> <uri:shared-0>}")).isTrue();
        assertThat(client.ask("ask {<uri:shared-1> <uri:shared-1> <uri:shared-1>}")).isTrue();
        assertThat(client.ask("ask {<" + uris.root() + "> schema:dateModified  \"1970-01-01T00:01:00Z\"^^xsd:dateTime}")).isTrue();
    }

    @Test
    public void testDeleteEntities() {
        URL q513Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q513-munged.ttl");
        Integer q513mutations = client.loadUrl(q513Location.toString());
        assertThat(q513mutations).isPositive();

        URL q42Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q42-munged.ttl");
        Integer q42Mutations = client.loadUrl(q42Location.toString());
        assertThat(q42Mutations).isPositive();

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, uris);
        entityIdsToDelete.add("Q513");
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        Instant avgEventTime = Instant.EPOCH.plus(2, ChronoUnit.MINUTES);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);

        assertThat(rdfPatchResult.getActualMutations()).isZero();
        assertThat(rdfPatchResult.getExpectedMutations()).isZero();
        assertThat(rdfPatchResult.getActualSharedElementsMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getPossibleSharedElementMutations()).isOne();
        assertThat(rdfPatchResult.getDeleteMutations()).isEqualTo(1880);

        assertThat(client.ask("ask { ?x <http://schema.org/about> <http://www.wikidata.org/entity/Q42>}")).isTrue();
        assertThat(client.ask("ask { ?x <http://schema.org/about> <http://www.wikidata.org/entity/Q513>}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/Q42> ?x ?y}")).isTrue();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/Q513> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/Q513-106C6F66-7E26-4113-9485-F599D2136B9B> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/Q42-586d443e-43ef-fdc2-223f-c4ff6c2b6531> ?x ?y}")).isTrue();
        assertThat(client.ask("ask { <https://en.wikipedia.org/wiki/Mount_Everest> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <https://en.wikipedia.org/> wikibase:wikiGroup \"wikipedia\"}")).isTrue();
        assertThat(client.ask("ask { <https://en.wikipedia.org/wiki/Douglas_Adams> ?x ?y}")).isTrue();
        assertThat(client.ask("ask {<" + uris.root() + "> schema:dateModified  \"1970-01-01T00:02:00Z\"^^xsd:dateTime}")).isTrue();
    }

    private int loadDumpIntoRepo(Reader dumpReader) throws IOException, InterruptedException, RDFParseException, RDFHandlerException {
        Munger munger = Munger.builder(uris).build();
        File file = File.createTempFile("munge-test", ".ttl");
        String fileURL = file.toURI().toURL().toString();
        Munge munge = new Munge(uris, munger, dumpReader, Integer.MAX_VALUE, file.getAbsolutePath());
        munge.run();
        return client.loadUrl(fileURL);
    }

    @Test
    public void testDeleteLexemes() throws RDFHandlerException, IOException, InterruptedException, RDFParseException {
        URL l4082Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "L4082-munged.ttl");

        loadDumpIntoRepo(StreamUtils.utf8(
                RdfRepositoryUpdaterIntegrationTest.class.getResourceAsStream(
                        RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "L691820.ttl")));
        Integer l4082mutations = client.loadUrl(l4082Location.toString());
        assertThat(l4082mutations).isPositive();

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, uris);
        entityIdsToDelete.add("L4082");
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        Instant avgEventTime = Instant.EPOCH.plus(2, ChronoUnit.MINUTES);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);

        assertThat(rdfPatchResult.getActualMutations()).isZero();
        assertThat(rdfPatchResult.getExpectedMutations()).isZero();

        assertThat(client.ask("ask { ?x ?p <http://www.wikidata.org/entity/L4082>}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/L4082-F1-DE357278-12EC-4D43-B13C-3B1750522397> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/L4082-S1-E7FFD9B4-DAC4-405A-864E-57963B5EA5AD> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/L4082-F1> ?x ?y}")).isFalse();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/L4082-S1> ?x ?y}")).isFalse();

        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/L691820-F1-52d7dca7-4f85-ba56-10f2-6beec35a235a> ?x ?y}")).isTrue();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/statement/L691820-S1-1b109a1c-449a-b255-ab8c-2aac8d6034d4> ?x ?y}")).isTrue();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/L691820-F1> ?x ?y}")).isTrue();
        assertThat(client.ask("ask { <http://www.wikidata.org/entity/L691820-S1> ?x ?y}")).isTrue();
    }

    @Test
    public void testReconcileEntities() throws RDFHandlerException, IOException, RDFParseException {
        // Import a valid version of the Q42 entity
        URL q42Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q42-munged.ttl");
        Integer q42Mutations = client.loadUrl(q42Location.toString());
        assertThat(q42Mutations).isPositive();

        // Distort it
        q42Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q42-borked.ttl");
        q42Mutations += client.loadUrl(q42Location.toString());
        client.update("DELETE DATA { wds:Q42-b47f944e-409a-8c99-5191-d27a1dd1ac38 ps:P4326 \"215854\" . }");
        assertThat(q42Mutations).isPositive();

        // Make sure we actually messed-up the entity
        assertThat(client.ask("ask { wd:Q42 schema:version \"1276705613\"^^xsd:integer}")).isTrue();
        assertThat(client.ask("ask { wd:Q42 schema:version \"1276705620\"^^xsd:integer}")).isTrue();
        StatementCollector collector = new StatementCollector();
        RDFParserSuppliers.defaultRdfParser().get(collector).parse(
                this.getClass().getResourceAsStream(RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q42-munged.ttl"),
                "uri:unused");
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, uris);
        entitiesToReconcile.put("Q42", collector.getStatements());
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, null);
        assertThat(rdfPatchResult.getActualMutations()).isZero();
        assertThat(rdfPatchResult.getExpectedMutations()).isZero();
        assertThat(rdfPatchResult.getReconciliationMutations()).isPositive();

        assertThat(client.ask("ask { wd:Q42 schema:version \"1276705613\"^^xsd:integer}")).isFalse();
        assertThat(client.ask("ask { wd:Q42 schema:version \"1276705620\"^^xsd:integer}")).isTrue();
        assertThat(client.ask("ask { wd:Q42 schema:dateModified \"2020-09-12T16:11:27Z\"^^xsd:dateTime}")).isFalse();
        assertThat(client.ask("ask { wd:Q42 schema:dateModified \"2020-09-13T16:11:27Z\"^^xsd:dateTime}")).isTrue();
        assertThat(client.ask("ask { wds:Q42-b47f944e-409a-8c99-5191-d27a1dd1ac38 ps:P4326 \"215853\" }")).isTrue();
        assertThat(client.ask("ask { wds:Q42-b47f944e-409a-8c99-5191-d27a1dd1ac38 ps:P4326 \"215854\" }")).isFalse();
        assertThat(client.ask("ask { wds:Q42-f03a5d7b-4ca7-a069-76fc-c864090ce051 a wikibase:BestRank }")).isFalse();
    }
}
