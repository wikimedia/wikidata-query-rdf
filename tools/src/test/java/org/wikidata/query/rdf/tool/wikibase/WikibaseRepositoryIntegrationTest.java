package org.wikidata.query.rdf.tool.wikibase;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.apache.kafka.common.requests.DeleteAclsResponse.log;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.test.CloseableRule.autoClose;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris.DEFAULT_API_PATH;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.test.CloseableRule;
import org.wikidata.query.rdf.tool.AbstractUpdaterIntegrationTestBase;
import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.tool.utils.NullStreamDumper;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.RecentChange;

import com.codahale.metrics.MetricRegistry;
import com.github.tomakehurst.wiremock.common.ClasspathFileSource;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.google.common.collect.ImmutableSet;

import lombok.SneakyThrows;

/**
 * Tests WikibaseRepository using the recorded responses from test instance of Wikidata. Note that we
 * can't delete or perform revision deletes so we can't test that part.
 *
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class WikibaseRepositoryIntegrationTest {

    private static final Instant START_TIME = Instant.ofEpochMilli(1579683006580L);
    private static final Instant ERROR_RESPONSE_INSTANT = Instant.ofEpochMilli(1579683018118L);

    @ClassRule
    public static WireMockClassRule wireMockRule =
            new WireMockClassRule(options()
                    .dynamicPort()
                    .fileSource(new ClasspathFileSource("WikibaseRepositoryIntegrationTest")));
    @Rule
    public final CloseableRule<WikibaseRepository> repo = autoClose(constructRepository(wireMockRule.baseUrl()));


    @SneakyThrows
    private static WikibaseRepository constructRepository(String uri) {
        return new WikibaseRepository(
                new WikibaseRepository.Uris(new URI(uri), ImmutableSet.of(0L, 120L), "/w/api.php", "/wiki/Special:EntityData/"),
                false, new MetricRegistry(), new NullStreamDumper(), null, RDFParserSuppliers.defaultRdfParser());
    }

    private final UrisScheme uris = UrisSchemeFactory.forHost("test.wikidata.org");

    private final URI baseUri;

    private final HttpClient client;

    public WikibaseRepositoryIntegrationTest() throws URISyntaxException {
        baseUri = new URI(wireMockRule.baseUrl());
        client = HttpClientBuilder.create().setUserAgent(this.getClass().getName() + " (integration test bot)").build();
    }

    @Test
    public void recentChangesWithLotsOfChangesHasContinue() throws RetryableException {
        /*
         * This relies on there being lots of changes in the past 30 days.
         */
        int batchSize = 15;
        RecentChangeResponse changes = repo.get().fetchRecentChanges(
                START_TIME.minus(30, ChronoUnit.DAYS), null, batchSize);
        assertNotNull(changes.getContinue());
        assertNotNull(changes.getContinue());
        assertNotNull(changes.getQuery());
        RecentChangeResponse.Query query = changes.getQuery();
        assertNotNull(query.getRecentChanges());
        List<RecentChange> recentChanges = changes.getQuery().getRecentChanges();
        assertThat(recentChanges, hasSize(batchSize));
        for (RecentChange rc : recentChanges) {
            assertThat(rc.getNs(), either(equalTo(0L)).or(equalTo(120L)));
            assertNotNull(rc.getTitle());
            assertNotNull(rc.getTimestamp());
            assertNotNull(rc.getRevId());
        }
        final Instant nextDate = repo.get().getChangeFromContinue(changes.getContinue()).timestamp();
        changes = repo.get().fetchRecentChanges(nextDate, null, batchSize);
        assertNotNull(changes.getQuery());
        assertNotNull(changes.getQuery().getRecentChanges());
    }

    @Test
    public void recentChangesWithFewChangesHasNoContinue() throws RetryableException {
        /*
         * This relies on there being very few changes in the current
         * second.
         */
        RecentChangeResponse changes = repo.get().fetchRecentChanges(START_TIME, null, 500);
        assertNull(changes.getContinue());
        assertNotNull(changes.getQuery());
        assertNotNull(changes.getQuery().getRecentChanges());
    }

    @Test
    public void aItemEditShowsUpInRecentChanges() throws RetryableException, ContainedException, IOException, URISyntaxException {
        editShowsUpInRecentChangesTestCase("QueryTestItem", "item");
    }

    @Test
    public void aPropertyEditShowsUpInRecentChanges() throws RetryableException, ContainedException, IOException, URISyntaxException {
        editShowsUpInRecentChangesTestCase("QueryTestProperty", "property");
    }

    private List<RecentChange> getRecentChanges(Instant date, int batchSize) throws RetryableException,
        ContainedException {
        RecentChangeResponse result = repo.get().fetchRecentChanges(date, null, batchSize);
        return result.getQuery().getRecentChanges();
    }

    @SuppressWarnings({ "rawtypes" })
    private void editShowsUpInRecentChangesTestCase(String label, String type) throws RetryableException,
            ContainedException, IOException, URISyntaxException {
        String entityId = firstEntityIdForLabelStartingWith(baseUri, label, "en", type);
        List<RecentChange> changes = getRecentChanges(START_TIME.minusSeconds(10), 10);
        boolean found = false;
        String title = entityId;
        if (type.equals("property")) {
            title = "Property:" + title;
        }
        for (RecentChange change : changes) {
            if (change.getTitle().equals(title)) {
                found = true;
                assertNotNull(change.getRevId());
                break;
            }
        }
        assertTrue("Didn't find new page in recent changes", found);
        Collection<Statement> statements = repo.get().fetchRdfForEntity(entityId);
        found = false;
        for (Statement statement : statements) {
            if (statement.getSubject().stringValue().equals(uris.entityIdToURI(entityId))) {
                found = true;
                break;
            }
        }
        assertTrue("Didn't find entity information in rdf", found);
    }

    @Test
    public void fetchIsNormalized() throws RetryableException, ContainedException, IOException, URISyntaxException {
        try (WikibaseRepository proxyRepo = AbstractUpdaterIntegrationTestBase.constructWikibaseRepository(wireMockRule.baseUrl())) {
            String entityId = firstEntityIdForLabelStartingWith(baseUri, "QueryTestItem", "en", "item");
            Collection<Statement> statements = proxyRepo.fetchRdfForEntity(entityId);
            boolean foundBad = false;
            boolean foundGood = false;
            for (Statement statement : statements) {
                if (statement.getObject().stringValue().contains("http://www.wikidata.org/ontology-beta#")) {
                    foundBad = true;
                }
                if (statement.getObject().stringValue().contains("http://www.wikidata.org/ontology-0.0.1#")) {
                    foundBad = true;
                }
                if (statement.getObject().stringValue().contains("http://www.wikidata.org/ontology#")) {
                    foundBad = true;
                }
                if (statement.getObject().stringValue().contains("http://wikiba.se/ontology#")) {
                    foundGood = true;
                }
            }
            assertTrue("Did not find correct ontology statements", foundGood);
            assertFalse("Found incorrect ontology statements", foundBad);
        }
    }

    @Test
    public void continueWorks() throws ContainedException, InterruptedException, URISyntaxException, IOException, RetryableException {
        String entityId = firstEntityIdForLabelStartingWith(baseUri, "QueryTestItem", "en", "item");
        List<RecentChange> changes = getRecentChanges(START_TIME.minusSeconds(10), 10);
        Change change = null;
        Long oldRevid = 0L;
        Long oldRcid = 0L;

        for (RecentChange rc : changes) {
            if (rc.getTitle().equals(entityId)) {
                oldRevid = rc.getRevId();
                oldRcid = rc.getRcId();
                change = new Change(rc.getTitle(), oldRevid, rc.getTimestamp(), oldRcid);
                break;
            }
        }
        assertNotNull("Did not find the first edit", change);
        // Ensure this change is in different second
        // make new edit now
        changes = getRecentChanges(change.timestamp().plusSeconds(1), 10);
        // check that new result does not contain old edit but contains new edit
        boolean found = false;
        for (RecentChange rc: changes) {
            if (rc.getTitle().equals(entityId)) {
                assertNotEquals("Found old edit after continue: revid", oldRevid, rc.getRevId());
                assertNotEquals("Found old edit after continue: offset", oldRcid, rc.getRcId());
                found = true;
            }
        }
        assertTrue("Did not find new edit", found);
    }

    @Test
    @SuppressWarnings({ "rawtypes" })
    public void recentChangesWithErrors() throws RetryableException, ContainedException {
        RecentChangeResponse changes = repo.get().fetchRecentChanges(ERROR_RESPONSE_INSTANT, null, 500);

        assertNull(changes.getContinue());
        assertNotNull(changes.getQuery());
        assertNotNull(changes.getQuery().getRecentChanges());
    }

    /**
     * Get the first id with the provided label in the provided language.
     */
    private String firstEntityIdForLabelStartingWith(URI baseURI, String label, String language, String type) throws URISyntaxException, IOException {

        URIBuilder builder = new URIBuilder(baseURI);
        builder.setPath(baseURI.getPath() + DEFAULT_API_PATH);
        builder.addParameter("format", "json");
        builder.addParameter("action", "wbsearchentities");
        builder.addParameter("search", label);
        builder.addParameter("language", language);
        builder.addParameter("type", type);
        URI uri = builder.build();
        log.debug("Searching for entity using {}", uri);
        SearchResponse result = getJson(new HttpGet(uri), SearchResponse.class);
        List<SearchResponse.SearchResult> resultList = result.getSearch();
        if (resultList.isEmpty()) {
            return null;
        }
        return resultList.get(0).getId();
    }

    private <T extends WikibaseResponse> T getJson(HttpRequestBase request, Class<T> valueType)
            throws IOException {
        try (CloseableHttpResponse response = (CloseableHttpResponse) client.execute(request)) {
            InputStream content = response.getEntity().getContent();
            return MapperUtils.getObjectMapper().readValue(content, valueType);
        }
    }
    // TODO we should verify the RDF dump format against a stored file
}
