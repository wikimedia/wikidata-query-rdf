package org.wikidata.query.rdf.tool.wikibase;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.wikidata.query.rdf.test.CloseableRule.autoClose;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.test.CloseableRule;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.RecentChange;

import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Tests WikibaseRepository using the beta instance of Wikidata. Note that we
 * can't delete or perform revision deletes so we can't test that part.
 */
public class WikibaseRepositoryIntegrationTest extends RandomizedTest {
    private static final String HOST = "test.wikidata.org";
    @Rule
    public final CloseableRule<WikibaseRepository> repo = autoClose(new WikibaseRepository("https://" + HOST));
    private final CloseableRule<WikibaseRepository> proxyRepo = autoClose(new WikibaseRepository("http://localhost:8812"));
    private final WikibaseUris uris = WikibaseUris.forHost(HOST);

    @Test
    @SuppressWarnings("unchecked")
    public void recentChangesWithLotsOfChangesHasContinue() throws RetryableException, ParseException {
        /*
         * This relies on there being lots of changes in the past 30 days. Which
         * is probably ok.
         */
        int batchSize = randomIntBetween(3, 30);
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
    @SuppressWarnings("unchecked")
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
    public void aItemEditShowsUpInRecentChanges() throws RetryableException, ContainedException {
        editShowsUpInRecentChangesTestCase("QueryTestItem", "item");
    }

    @Test
    public void aPropertyEditShowsUpInRecentChanges() throws RetryableException, ContainedException {
        editShowsUpInRecentChangesTestCase("QueryTestProperty", "property");
    }

    private List<RecentChange> getRecentChanges(Instant date, int batchSize) throws RetryableException,
        ContainedException {
        // Add a bit of a wait to try and improve Jenkins test stability.
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            // nothing to do here, sorry. I know it looks bad.
        }
        RecentChangeResponse result = repo.get().fetchRecentChanges(date, null, batchSize);
        return result.getQuery().getRecentChanges();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void editShowsUpInRecentChangesTestCase(String label, String type) throws RetryableException,
            ContainedException {
        long now = System.currentTimeMillis();
        String entityId = repo.get().firstEntityIdForLabelStartingWith(label, "en", type);
        repo.get().setLabel(entityId, type, label + now, "en");
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
            if (statement.getSubject().stringValue().equals(uris.entity() + entityId)) {
                found = true;
                break;
            }
        }
        assertTrue("Didn't find entity information in rdf", found);
    }

    @Test
    public void fetchIsNormalized() throws RetryableException, ContainedException, IOException, URISyntaxException {
        long now = System.currentTimeMillis();
        try (WikibaseRepository proxyRepo = new WikibaseRepository(new URI("http://localhost:8812"))) {
            String entityId = repo.get().firstEntityIdForLabelStartingWith("QueryTestItem", "en", "item");
            repo.get().setLabel(entityId, "item", "QueryTestItem" + now, "en");
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
    public void continueWorks() throws RetryableException, ContainedException, ParseException, InterruptedException {
        long now = System.currentTimeMillis();
        String entityId = repo.get().firstEntityIdForLabelStartingWith("QueryTestItem", "en", "item");
        repo.get().setLabel(entityId, "item", "QueryTestItem" + now, "en");
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
        repo.get().setLabel(entityId, "item", "QueryTestItem" + now + "updated", "en");
        changes = getRecentChanges(change.timestamp().plusSeconds(1), 10);
        // check that new result does not contain old edit but contains new edit
        boolean found = false;
        for (RecentChange rc: changes) {
            if (rc.getTitle().equals(entityId)) {
                assertNotEquals("Found old edit after continue: revid", oldRevid, rc.getRevId());
                assertNotEquals("Found old edit after continue: rcid", oldRcid, rc.getRcId());
                found = true;
            }
        }
        assertTrue("Did not find new edit", found);
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void recentChangesWithErrors() throws RetryableException, ContainedException {
        RecentChangeResponse changes = proxyRepo.get().fetchRecentChanges(Instant.now(), null, 500);
        assertNull(changes.getContinue());
        assertNotNull(changes.getQuery());
        assertNotNull(changes.getQuery().getRecentChanges());
    }

    // TODO we should verify the RDF dump format against a stored file
}
