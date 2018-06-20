package org.wikidata.query.rdf.tool.rdf.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.wikidata.query.rdf.tool.Update.buildHttpClientRetryer;

import java.net.URI;
import java.time.Duration;

import org.eclipse.jetty.client.HttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class RdfClientUnitTest {

    @Rule public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

    private HttpClient httpClient;
    private RdfClient rdfClient;

    @Before
    public void initializeClient() throws Exception {
        URI uri = URI.create("http://localhost:" + wireMockRule.port());
        httpClient = new HttpClient();
        httpClient.start();
        rdfClient = new RdfClient(httpClient, uri, buildHttpClientRetryer(), Duration.of(20, MILLIS));
    }

    @Test(expected = ContainedException.class)
    public void throwsExceptionOn500Response() {
        stubFor(post("/")
                .willReturn(aResponse()
                    .withStatus(500))
        );

        rdfClient.query("invalid query");
    }

    // @Ignore("long running test, enable it to test changes to retry mechanics")
    @Test(expected = FatalException.class)
    public void retriesOnTimeout() {
        stubFor(post("/")
                .willReturn(aResponse().withStatus(500).withFixedDelay(25))
        );

        rdfClient.query("invalid query");

        verify(6, postRequestedFor(urlEqualTo("/")));
    }

    @After
    public void stopHttpClient() throws Exception {
        httpClient.stop();
    }

}
