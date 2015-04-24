package org.wikidata.query.rdf.tool.wikibase;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Tests WikibaseRepository using the beta instance of Wikidata. Note that we
 * can't delete or perform revision deletes so we can't test that part.
 */
public class WikibaseRepositoryIntegrationTest extends RandomizedTest {
    private static final String HOST = "test.wikidata.org";
    private final WikibaseRepository repo = new WikibaseRepository("http", HOST);
    private final WikibaseUris uris = new WikibaseUris(HOST);

    @Test
    @SuppressWarnings("unchecked")
    public void recentChangesWithLotsOfChangesHasContinue() throws RetryableException {
        /*
         * This relies on there being lots of changes in the past 30 days. Which
         * is probably ok.
         */
        int batchSize = randomIntBetween(3, 30);
        JSONObject changes = repo.fetchRecentChanges(new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30)),
                null, batchSize);
        Map<String, Object> c = changes;
        assertThat(c, hasKey("continue"));
        assertThat((Map<String, Object>) changes.get("continue"), hasKey("rccontinue"));
        assertThat(c, hasKey("query"));
        Map<String, Object> query = (Map<String, Object>) c.get("query");
        assertThat(query, hasKey("recentchanges"));
        List<Object> recentChanges = (JSONArray) ((Map<String, Object>) c.get("query")).get("recentchanges");
        assertThat(recentChanges, hasSize(batchSize));
        for (Object rco : recentChanges) {
            Map<String, Object> rc = (Map<String, Object>) rco;
            assertThat(rc, hasEntry(equalTo("ns"), either(equalTo((Object) 0L)).or(equalTo((Object) 120L))));
            assertThat(rc, hasEntry(equalTo("title"), instanceOf(String.class)));
            assertThat(rc, hasEntry(equalTo("timestamp"), instanceOf(String.class)));
            assertThat(rc, hasEntry(equalTo("revid"), instanceOf(Long.class)));
        }
        changes = repo.fetchRecentChanges(null /* ignored */, (JSONObject) changes.get("continue"), batchSize);
        assertThat(c, hasKey("query"));
        assertThat((Map<String, Object>) c.get("query"), hasKey("recentchanges"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void recentChangesWithFewChangesHasNoContinue() throws RetryableException {
        /*
         * This relies on there being very few changes in the current
         * millisecond.
         */
        JSONObject changes = repo.fetchRecentChanges(new Date(System.currentTimeMillis()), null, 500);
        Map<String, Object> c = changes;
        assertThat(c, not(hasKey("continue")));
        assertThat(c, hasKey("query"));
        assertThat((Map<String, Object>) c.get("query"), hasKey("recentchanges"));
    }

    @Test
    public void aItemEditShowsUpInRecentChanges() throws RetryableException, ContainedException {
        editShowsUpInRecentChangesTestCase("QueryTestItem", "item");
    }

    @Test
    public void aPropertyEditShowsUpInRecentChanges() throws RetryableException, ContainedException {
        editShowsUpInRecentChangesTestCase("QueryTestProperty", "property");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void editShowsUpInRecentChangesTestCase(String label, String type) throws RetryableException,
            ContainedException {
        long now = System.currentTimeMillis();
        String entityId = repo.firstEntityIdForLabelStartingWith(label, "en", type);
        repo.setLabel(entityId, type, label + now, "en");
        JSONObject result = repo.fetchRecentChanges(new Date(now), null, 10);
        JSONArray changes = (JSONArray) ((JSONObject) result.get("query")).get("recentchanges");
        boolean found = false;
        String title = entityId;
        if (type.equals("property")) {
            title = "Property:" + title;
        }
        for (Object changeObject : changes) {
            JSONObject change = (JSONObject) changeObject;
            if (change.get("title").equals(title)) {
                found = true;
                Map<String, Object> c = change;
                assertThat(c, hasEntry(equalTo("revid"), isA((Class) Long.class)));
                break;
            }
        }
        assertTrue("Didn't find new page in recent changes", found);
        Collection<Statement> statements = repo.fetchRdfForEntity(entityId);
        found = false;
        for (Statement statement : statements) {
            if (statement.getSubject().stringValue().equals(uris.entity() + entityId)) {
                found = true;
                break;
            }
        }
        assertTrue("Didn't find entity information in rdf", found);
    }

    @Test
    public void fetchIsNormalized() throws RetryableException, ContainedException {
        long now = System.currentTimeMillis();
        String entityId = repo.firstEntityIdForLabelStartingWith("QueryTestItem", "en", "item");
        repo.setLabel(entityId, "item", "QueryTestItem" + now, "en");
        Collection<Statement> statements = repo.fetchRdfForEntity(entityId);
        boolean foundBad = false, foundGood = false;
        for (Statement statement : statements) {
            if(statement.getObject().stringValue().contains("http://www.wikidata.org/ontology-beta#")) {
                foundBad = true;
            }
            if(statement.getObject().stringValue().contains("http://www.wikidata.org/ontology-0.0.1#")) {
                foundBad = true;
            }
            if(statement.getObject().stringValue().contains("http://www.wikidata.org/ontology#")) {
                foundGood = true;
            }
        }
        assertTrue("Did not find correct ontology statements", foundGood);
        assertFalse("Found incorrect ontology statements", foundBad);
    }
    // TODO we should verify the RDF dump format against a stored file
}
