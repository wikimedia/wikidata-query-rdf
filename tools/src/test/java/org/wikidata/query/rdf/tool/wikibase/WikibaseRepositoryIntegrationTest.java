package org.wikidata.query.rdf.tool.wikibase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;

/**
 * Tests WikibaseRepository using the beta instance of Wikidata. Note that we
 * can't delete or perform revision deletes so we can't test that part.
 */
public class WikibaseRepositoryIntegrationTest {
    private static final String HOST = "test.wikidata.org";
    private final WikibaseRepository repo = new WikibaseRepository("http", HOST);
    private final Entity entityUris = new Entity(HOST);

    @Test
    @SuppressWarnings("unchecked")
    public void recentChangesWithLotsOfChangesHasContinue() throws RetryableException {
        /*
         * This relies on there being lots of changes in the past 30 days. Which
         * is probably ok.
         */
        JSONObject changes = repo.fetchRecentChanges(new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30)),
                null);
        Map<String, Object> c = changes;
        assertThat(c, hasKey("continue"));
        assertThat((Map<String, Object>) changes.get("continue"), hasKey("rccontinue"));
        assertThat((Map<String, Object>) c.get("continue"), hasKey("continue"));
        assertThat(c, hasKey("query"));
        assertThat((Map<String, Object>) c.get("query"), hasKey("recentchanges"));
        changes = repo.fetchRecentChanges(null /* ignored */, (JSONObject) changes.get("continue"));
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
        JSONObject changes = repo.fetchRecentChanges(new Date(System.currentTimeMillis()), null);
        Map<String, Object> c = changes;
        assertThat(c, not(hasKey("continue")));
        assertThat(c, hasKey("query"));
        assertThat((Map<String, Object>) c.get("query"), hasKey("recentchanges"));
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void anEditsShowsUpInRecentChanges() throws RetryableException, ContainedException {
        long now = System.currentTimeMillis();
        String label = "QueryTest" + now;
        String entityId = repo.addPage(label);
        JSONObject result = repo.fetchRecentChanges(new Date(now), null);
        JSONArray changes = (JSONArray) ((JSONObject) result.get("query")).get("recentchanges");
        boolean found = false;
        for (Object changeObject : changes) {
            JSONObject change = (JSONObject) changeObject;
            if (change.get("title").equals(entityId)) {
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
            if (statement.getSubject().stringValue().equals(entityUris.namespace() + entityId)) {
                found = true;
                break;
            }
        }
        assertTrue("Didn't find entity information in rdf", found);
    }

    // TODO we should verify the RDF dump format against a stored file
}
