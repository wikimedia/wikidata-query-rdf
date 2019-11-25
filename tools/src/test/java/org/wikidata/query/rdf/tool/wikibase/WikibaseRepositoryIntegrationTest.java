package org.wikidata.query.rdf.tool.wikibase;

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
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris.API_URL;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.test.CloseableRule;
import org.wikidata.query.rdf.test.Randomizer;
import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.RecentChange;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;

/**
 * Tests WikibaseRepository using the beta instance of Wikidata. Note that we
 * can't delete or perform revision deletes so we can't test that part.
 * TODO: rewrite this test, it depends on test.wikidata.org and performs edits to it
 * concurrent executions might cause edit conflicts.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class WikibaseRepositoryIntegrationTest {
    private static final String HOST = "test.wikidata.org";
    private static final String PROXY_URI_STRING = "http://localhost:8812";
    private final String baseUriString = "https://" + HOST;
    @Rule
    public final Randomizer randomizer = new Randomizer();
    @Rule
    public final CloseableRule<WikibaseRepository> repo = autoClose(new WikibaseRepository(baseUriString, new MetricRegistry()));
    private final CloseableRule<WikibaseRepository> proxyRepo = autoClose(new WikibaseRepository(PROXY_URI_STRING, new MetricRegistry()));
    private final UrisScheme uris = UrisSchemeFactory.forHost(HOST);
    private final URI baseUri;
    private final HttpClient client;

    public WikibaseRepositoryIntegrationTest() throws URISyntaxException {
        baseUri = new URI(baseUriString);
        client = HttpClientBuilder.create().setUserAgent(this.getClass().getName() + " (integration test bot)").build();
    }

    @Test
    public void recentChangesWithLotsOfChangesHasContinue() throws RetryableException {
        /*
         * This relies on there being lots of changes in the past 30 days. Which
         * is probably ok.
         */
        int batchSize = randomizer.randomIntBetween(3, 30);
        RecentChangeResponse changes = repo.get().fetchRecentChanges(
                Instant.now().minus(30, ChronoUnit.DAYS), null, batchSize);
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
        RecentChangeResponse changes = repo.get().fetchRecentChanges(Instant.now(), null, 500);
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
        // Add a bit of a wait to try and improve Jenkins test stability.
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            // nothing to do here, sorry. I know it looks bad.
        }
        RecentChangeResponse result = repo.get().fetchRecentChanges(date, null, batchSize);
        return result.getQuery().getRecentChanges();
    }

    @SuppressWarnings({ "rawtypes" })
    private void editShowsUpInRecentChangesTestCase(String label, String type) throws RetryableException,
            ContainedException, IOException, URISyntaxException {
        long now = System.currentTimeMillis();
        String entityId = firstEntityIdForLabelStartingWith(baseUri, label, "en", type);
        setLabel(baseUri, entityId, type, label + now, "en");
        List<RecentChange> changes = getRecentChanges(Instant.now().minusSeconds(10), 10);
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
        long now = System.currentTimeMillis();
        try (WikibaseRepository proxyRepo = new WikibaseRepository(new URI("http://localhost:8812"), new MetricRegistry())) {
            String entityId = firstEntityIdForLabelStartingWith(baseUri, "QueryTestItem", "en", "item");
            setLabel(baseUri, entityId, "item", "QueryTestItem" + now, "en");
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
        long now = System.currentTimeMillis();
        String entityId = firstEntityIdForLabelStartingWith(baseUri, "QueryTestItem", "en", "item");
        setLabel(baseUri, entityId, "item", "QueryTestItem" + now, "en");
        List<RecentChange> changes = getRecentChanges(Instant.now().minusSeconds(10), 10);
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
        Thread.sleep(1000);
        // make new edit now
        setLabel(baseUri, entityId, "item", "QueryTestItem" + now + "updated", "en");
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
        RecentChangeResponse changes = proxyRepo.get().fetchRecentChanges(Instant.now(), null, 500);
        assertNull(changes.getContinue());
        assertNotNull(changes.getQuery());
        assertNotNull(changes.getQuery().getRecentChanges());
    }

    /**
     * Uri to which you can post to edit an entity.
     *
     * @param entityId the id to edit
     * @param newType the type of the entity to create. Ignored if entityId
     *            is not null.
     * @param data data to add to the entity
     */
    private URI edit(URI baseURI, String entityId, String newType, String data) throws URISyntaxException {
        URIBuilder builder = new URIBuilder(baseURI);
        builder.setPath(baseURI.getPath() + API_URL);
        builder.addParameter("format", "json");
        builder.addParameter("action", "wbeditentity");
        if (entityId != null) {
            builder.addParameter("id", entityId);
        } else {
            builder.addParameter("new", newType);
        }
        builder.addParameter("data", data);
        return builder.build();
    }

    private String setLabel(URI baseUri, String entityId, String type, String label, String language) throws URISyntaxException, IOException {
        String datatype = type.equals("property") ? "string" : null;

        EditRequest data = new EditRequest(
                datatype,
                ImmutableMap.of(
                        language,
                        new EditRequest.Label(language, label)));

        URI uri = edit(baseUri, entityId, type, MapperUtils.getObjectMapper().writeValueAsString(data));
        log.debug("Editing entity using {}", uri);
        EditResponse result = getJson(postWithToken(baseUri, uri), EditResponse.class);
        return result.getEntity().getId();
    }

    /**
     * Post with a csrf token.
     *
     * @throws IOException if its thrown while communicating with wikibase
     */
    private HttpPost postWithToken(URI baseUri, URI uri) throws IOException, URISyntaxException {
        HttpPost request = new HttpPost(uri);
        List<NameValuePair> entity = new ArrayList<>();
        entity.add(new BasicNameValuePair("token", csrfToken(baseUri)));
        request.setEntity(new UrlEncodedFormEntity(entity, Consts.UTF_8));
        return request;
    }

    /**
     * Get the first id with the provided label in the provided language.
     */
    private String firstEntityIdForLabelStartingWith(URI baseURI, String label, String language, String type) throws URISyntaxException, IOException {

        URIBuilder builder = new URIBuilder(baseURI);
        builder.setPath(baseURI.getPath() + API_URL);
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

    /**
     * Fetch a csrf token.
     *
     * @throws IOException if its thrown while communicating with wikibase
     */
    private String csrfToken(URI baseUri) throws IOException, URISyntaxException {
        URIBuilder builder = new URIBuilder(baseUri);
        builder.setPath(baseUri.getPath() + API_URL);
        builder.addParameter("format", "json");
        builder.setParameter("action", "query");
        builder.setParameter("meta", "tokens");
        builder.setParameter("continue", "");
        URI uri = builder.build();
        log.debug("Fetching csrf token from {}", uri);
        return getJson(new HttpGet(uri), CsrfTokenResponse.class).getQuery().getTokens().getCsrfToken();
    }

    private <T extends WikibaseResponse> T getJson(HttpRequestBase request, Class<T> valueType)
            throws IOException {
        try (CloseableHttpResponse response = (CloseableHttpResponse) client.execute(request)) {
            return MapperUtils.getObjectMapper().readValue(response.getEntity().getContent(), valueType);
        }
    }
    // TODO we should verify the RDF dump format against a stored file
}
