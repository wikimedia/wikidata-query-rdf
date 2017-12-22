package org.wikidata.query.rdf.tool.wikibase;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.inputDateFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.io.Resources;

public class WikibaseRepositoryWireIntegrationTest {

    @Rule public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .dynamicPort()
            .dynamicHttpsPort());
    private WikibaseRepository repository;


    @Before
    public void createWikibaseRepository() {
        repository = new WikibaseRepository("http", "localhost", wireMockRule.port());
    }

    @After
    public void shutdownWikibaseRepository() throws IOException {
        repository.close();
    }

    @Test
    public void recentChangesAreParsed() throws IOException, RetryableException, ParseException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withBody(load("recent_changes.json"))));

        RecentChangeResponse response = repository.fetchRecentChanges(new Date(), null, 10);

        assertThat(response.getContinue().getRcContinue(), is("20171126140446|634268213"));
        assertThat(response.getContinue().getContinue(), is("-||"));
        assertThat(response.getQuery().getRecentChanges(), hasSize(2));
        RecentChangeResponse.RecentChange change = response.getQuery().getRecentChanges().get(0);

        assertThat(change.getTitle(), is("Q16013051"));
        assertThat(change.getType(), is("edit"));
        assertThat(change.getNs(), is(0L));
        assertThat(change.getRevId(), is(598908952L));
        assertThat(change.getRcId(), is(634268202L));
        assertThat(change.getTimestamp(), is(inputDateFormat().parse("2017-11-26T14:04:45Z")));
    }

    @Test
    public void unknownFieldsAreIgnored() throws RetryableException, IOException {
        stubFor(get(anyUrl())
                .willReturn(aResponse().withBody(load("recent_changes_extra_fields.json"))));

        RecentChangeResponse response = repository.fetchRecentChanges(new Date(), null, 10);

        assertThat(response.getQuery().getRecentChanges(), hasSize(2));
        RecentChangeResponse.RecentChange change = response.getQuery().getRecentChanges().get(0);

        assertThat(change.getTitle(), is("Q16013051"));
    }

    private String load(String name) throws IOException {
        String prefix = this.getClass().getPackage().getName().replace(".", "/");
        return Resources.toString(getResource(prefix + "/" + name), UTF_8);
    }
}
