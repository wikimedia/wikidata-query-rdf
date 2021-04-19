package org.wikidata.query.rdf.tool.wikibase;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.test.SystemPropertyContext;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.tool.utils.NullStreamDumper;

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import lombok.SneakyThrows;

public class WikibaseRepositoryWireIntegrationTest {

    private WikibaseRepository repository;
    private int wiremockPort = Integer.parseInt(System.getProperty("wiremock.port"));

    @Before
    public void configureWireMock() {
        WireMock.configureFor("localhost", wiremockPort);
    }

    @SneakyThrows
    @Before
    public void createWikibaseRepository() {
        repository = new WikibaseRepository(
                new WikibaseRepository.Uris(new URI("http://localhost:" + wiremockPort), ImmutableSet.of(0L, 120L), "/w/api.php", "/wiki/Special:EntityData/"),
                false, new MetricRegistry(), new NullStreamDumper(), null, RDFParserSuppliers.defaultRdfParser());
    }

    @After
    public void shutdownWikibaseRepository() throws IOException {
        repository.close();
    }

    @SuppressWarnings("boxing")
    @Test
    public void recentChangesAreParsed() throws IOException, RetryableException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withBody(load("recent_changes.json"))));

        RecentChangeResponse response = repository.fetchRecentChanges(Instant.now(), null, 10);

        assertThat(response.getContinue().getRcContinue(), is("20171126140446|634268213"));
        assertThat(response.getContinue().getContinue(), is("-||"));
        assertThat(response.getQuery().getRecentChanges(), hasSize(2));
        RecentChangeResponse.RecentChange change = response.getQuery().getRecentChanges().get(0);

        assertThat(change.getTitle(), is("Q16013051"));
        assertThat(change.getType(), is("edit"));
        assertThat(change.getNs(), is(0L));
        assertThat(change.getRevId(), is(598908952L));
        assertThat(change.getRcId(), is(634268202L));
        assertThat(change.getTimestamp(), is(WikibaseRepository.INPUT_DATE_FORMATTER.parse("2017-11-26T14:04:45Z", Instant::from)));
    }

    @Test
    public void unknownFieldsAreIgnored() throws RetryableException, IOException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withBody(load("recent_changes_extra_fields.json"))));

        RecentChangeResponse response = repository.fetchRecentChanges(Instant.now(), null, 10);

        assertThat(response.getQuery().getRecentChanges(), hasSize(2));
        RecentChangeResponse.RecentChange change = response.getQuery().getRecentChanges().get(0);

        assertThat(change.getTitle(), is("Q16013051"));
    }

    private String load(String name) throws IOException {
        String prefix = this.getClass().getPackage().getName().replace(".", "/");
        return Resources.toString(getResource(prefix + "/" + name), UTF_8);
    }

    @Test
    public void noContentResponse() throws RetryableException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withStatus(204).withBody("")));
        assertThatThrownBy(() -> repository.fetchRdfForEntity("Q1", 123))
                .isInstanceOf(WikibaseEntityFetchException.class)
                .hasMessage("Cannot fetch entity at http://localhost:%d/wiki/Special:EntityData/Q1.ttl?flavor=dump&revision=123: NO_CONTENT", wiremockPort);
    }

    @Test
    public void rdfAndConstraints() throws RetryableException {
        repository.setCollectConstraints(true);
        stubFor(get(urlMatching("/wiki/Special:EntityData/Q2.ttl[?]flavor=dump.*"))
                .willReturn(aResponse().withBody("<a> <b> <c> .")));
        stubFor(get(urlMatching("/wiki/Q2[?]action=constraintsrdf&nocache=[0-9]+"))
                .willReturn(aResponse().withBody("<d> <e> <f> .")));
        Collection<Statement> response = repository.fetchRdfForEntity("Q2");
        assertThat(response, hasSize(2));
    }

    @Test
    public void rdfNewUsesRevisions() throws RetryableException {
        repository.setCollectConstraints(false);
        repository.setRevisionCutoff(Duration.of(3, ChronoUnit.HOURS));
        stubFor(get(urlMatching("/wiki/Special:EntityData/Q2.ttl[?]flavor=dump&revision=1234"))
                .willReturn(aResponse().withBody("<a> <b> <c> .")));
        Collection<Statement> response = repository.fetchRdfForEntity(new Change("Q2", 1234, Instant.now().minus(5, ChronoUnit.MINUTES), 0));
        assertThat(response, hasSize(1));
    }

    @Test
    public void rdfNewWithoutCutoff() throws RetryableException {
        repository.setCollectConstraints(false);
        repository.setRevisionCutoff(Duration.ZERO);
        stubFor(get(urlMatching("/wiki/Special:EntityData/Q2.ttl[?]flavor=dump&nocache=[0-9]+"))
                .willReturn(aResponse().withBody("<a> <b> <c> .")));
        Collection<Statement> response = repository.fetchRdfForEntity(new Change("Q2", 1234, Instant.now().minus(5, ChronoUnit.MINUTES), 0));
        assertThat(response, hasSize(1));
    }

    @Test
    public void rdfOldUsesNocache() throws RetryableException {
        repository.setCollectConstraints(false);
        repository.setRevisionCutoff(Duration.of(3, ChronoUnit.HOURS));
        stubFor(get(urlMatching("/wiki/Special:EntityData/Q2.ttl[?]flavor=dump&nocache=[0-9]+"))
                .willReturn(aResponse().withBody("<a> <b> <c> .")));
        Collection<Statement> response = repository.fetchRdfForEntity(new Change("Q2", 1234, Instant.now().minus(5, ChronoUnit.DAYS), 0));
        assertThat(response, hasSize(1));
    }

    @Test
    public void defaultUserAgentIsSet() throws RetryableException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withBody("<a> <b> <c> .")));

        repository.fetchRdfForEntity("Q1");

        verify(getRequestedFor(anyUrl()).withHeader("User-Agent", containing("Wikidata Query Service Updater")));
    }

    @Test
    public void userAgentIsSetFromSystemProperty() throws RetryableException, IOException {
        try (Closeable propContext = SystemPropertyContext.setProperty("http.userAgent", "some-agent")) {

            // recreate repository now that system prop is set
            createWikibaseRepository();

            stubFor(get(anyUrl())
                    .willReturn(aResponse().withBody("<a> <b> <c> .")));

            repository.fetchRdfForEntity("Q1");

            verify(getRequestedFor(anyUrl()).withHeader("User-Agent", containing("some-agent")));
        }
    }
}
