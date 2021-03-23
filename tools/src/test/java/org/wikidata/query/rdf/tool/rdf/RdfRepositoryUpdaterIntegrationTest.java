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

import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

public class RdfRepositoryUpdaterIntegrationTest {
    private final RdfClient client = new RdfClient(
            buildHttpClient(getHttpProxyHost(), getHttpProxyPort()), url("/namespace/wdq/sparql"),
            buildHttpClientRetryer(),
            getRdfClientTimeout()
    );

    private final UrisScheme uris = UrisSchemeFactory.getURISystem();
    List<String> entityIdsToDelete = new ArrayList<>();

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
                statements("uri:a", "uri:x"), statements("uri:ignored"), entityIdsToDelete);
        Instant avgEventTime = Instant.EPOCH.plus(1, ChronoUnit.MINUTES);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        assertThat(rdfPatchResult.getActualMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getExpectedMutations()).isEqualTo(3);
        assertThat(rdfPatchResult.getActualSharedElementsMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getPossibleSharedElementMutations()).isEqualTo(3);
        assertThat(rdfPatchResult.getDeleteMutations()).isEqualTo(0);
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
        assertThat(q513mutations).isGreaterThan(0);

        URL q42Location = getResource(RdfRepositoryUpdaterIntegrationTest.class,
                RdfRepositoryUpdaterIntegrationTest.class.getSimpleName() + "." + "Q42-munged.ttl");
        Integer q42Mutations = client.loadUrl(q42Location.toString());
        assertThat(q42Mutations).isGreaterThan(0);

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, uris);
        entityIdsToDelete.add("Q513");
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(), entityIdsToDelete);
        Instant avgEventTime = Instant.EPOCH.plus(2, ChronoUnit.MINUTES);
        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);

        assertThat(rdfPatchResult.getActualMutations()).isEqualTo(0);
        assertThat(rdfPatchResult.getExpectedMutations()).isEqualTo(0);
        assertThat(rdfPatchResult.getActualSharedElementsMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getPossibleSharedElementMutations()).isEqualTo(1);
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
}
