package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statements;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClient;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClientRetryer;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyHost;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyPort;
import static org.wikidata.query.rdf.tool.RdfRepositoryForTesting.url;
import static org.wikidata.query.rdf.tool.Update.getRdfClientTimeout;

import org.junit.Test;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

public class RdfRepositoryUpdaterIntegrationTest {
    private final RdfClient client = new RdfClient(
        buildHttpClient(getHttpProxyHost(), getHttpProxyPort()), url("/namespace/wdq/sparql"),
        buildHttpClientRetryer(),
        getRdfClientTimeout()
    );

    @Test
    public void test() {
        // prepare the test dataset
        client.update("DELETE DATA {" +
                "<uri:b> <uri:b> <uri:b> ." +
                "<uri:shared-1> <uri:shared-1> <uri:shared-1> ." +
                "};\n" +
                "INSERT DATA { <uri:a> <uri:a> <uri:a> .\n" +
                "<uri:shared-0> <uri:shared-0> <uri:shared-0> . };\n");

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
        RDFPatch patch = new RDFPatch(statements("uri:b"), statements("uri:shared-0", "uri:shared-1"),
                statements("uri:a", "uri:x"), statements("uri:ignored"));

        RDFPatchResult rdfPatchResult = rdfRepositoryUpdater.applyPatch(patch);
        assertThat(rdfPatchResult.getActualMutations()).isEqualTo(2);
        assertThat(rdfPatchResult.getExpectedMutations()).isEqualTo(3);
        assertThat(rdfPatchResult.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(rdfPatchResult.getPossibleSharedElementMutations()).isEqualTo(2);
        assertThat(client.ask("ask {<uri:a> <uri:a> <uri:a>}")).isFalse();
        assertThat(client.ask("ask {<uri:b> <uri:b> <uri:b>}")).isTrue();
        assertThat(client.ask("ask {<uri:shared-0> <uri:shared-0> <uri:shared-0>}")).isTrue();
        assertThat(client.ask("ask {<uri:shared-1> <uri:shared-1> <uri:shared-1>}")).isTrue();
    }
}
