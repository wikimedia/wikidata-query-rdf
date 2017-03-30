package org.wikidata.query.rdf.tool.change;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller.Batch;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

public class RecentChangesPollerUnitTest {
    private WikibaseRepository repository;

    private int batchSize = 10;

    /**
     * Mock return of the first batch call to repository.
     * @param startTime
     * @param result
     * @throws RetryableException
     */
    private void firstBatchReturns(Date startTime, JSONObject result) throws RetryableException {
        when(repository.fetchRecentChangesByTime(any(Date.class), eq(batchSize))).thenCallRealMethod();
        when(repository.fetchRecentChanges(any(Date.class), (JSONObject)eq(null), eq(batchSize))).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);
        when(repository.isValidEntity(any(String.class))).thenReturn(true);

    }

    /**
     * Check deduplication.
     * Create 20 changes, of which each two are dupes,
     * check that dupes are eliminated.
     * @throws RetryableException
     */
    @Test
    @SuppressWarnings("unchecked")
    public void dedups() throws RetryableException {
        Date startTime = new Date();
        // Build a result from wikibase with duplicate recent changes
        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);
        String date = WikibaseRepository.inputDateFormat().format(new Date());
        // 20 entries with 10 total Q ids
        for (int i = 0; i < 20; i++) {
            JSONObject rc = new JSONObject();
            rc.put("ns", Long.valueOf(0));
            rc.put("title", "Q" + (i / 2));
            rc.put("timestamp", date);
            rc.put("revid", Long.valueOf(i));
            rc.put("rcid", Long.valueOf(i));
            rc.put("type", "edit");
            recentChanges.add(rc);
        }

        firstBatchReturns(startTime, result);
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();

        assertThat(batch.changes(), hasSize(10));
        List<Change> changes = new ArrayList<>(batch.changes());
        Collections.sort(changes, new Comparator<Change>() {
            @Override
            public int compare(Change lhs, Change rhs) {
                return lhs.entityId().compareTo(rhs.entityId());
            }
        });
        for (int i = 0; i < 10; i++) {
            assertEquals(changes.get(i).entityId(), "Q" + i);
            assertEquals(changes.get(i).revision(), 2 * i + 1);
        }
    }

    /**
     * Check that continuing works.
     * Check that poller passes continuation to the next batch.
     * @throws RetryableException
     */
    @Test
    @SuppressWarnings("unchecked")
    public void continuePoll() throws RetryableException {
        // Use old date to remove backoff
        Date startTime = DateUtils.addDays(new Date(), -10);
        int batchSize = 10;

        JSONObject result = new JSONObject();
        JSONObject rc = new JSONObject();
        JSONArray recentChanges = new JSONArray();
        JSONObject query = new JSONObject();

        Date revDate = DateUtils.addSeconds(startTime, 20);
        String date = WikibaseRepository.inputDateFormat().format(revDate);
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q666");
        rc.put("timestamp", date);
        rc.put("revid", 1L);
        rc.put("rcid", 1L);
        rc.put("type", "edit");
        recentChanges.add(rc);
        rc = new JSONObject();
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q667");
        rc.put("timestamp", date);
        rc.put("revid", 7L);
        rc.put("rcid", 7L);
        rc.put("type", "edit");
        recentChanges.add(rc);

        query.put("recentchanges", recentChanges);
        result.put("query", query);

        JSONObject contJson = new JSONObject();
        contJson.put("rccontinue", outputDateFormat().format(revDate) + "|8");
        contJson.put("continue", "-||");
        result.put("continue", contJson);

        firstBatchReturns(startTime, result);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(2));
        assertEquals(7, batch.changes().get(1).rcid());
        assertEquals(date, WikibaseRepository.inputDateFormat().format(batch.leftOffDate()));
        assertEquals(contJson, batch.getLastContinue());

        ArgumentCaptor<Date> argumentDate = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<JSONObject> argumentJson = ArgumentCaptor.forClass(JSONObject.class);

        recentChanges.clear();
        when(repository.fetchRecentChanges(argumentDate.capture(), argumentJson.capture(), eq(batchSize))).thenReturn(result);
        // check that poller passes the continue object to the next batch
        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(0));
        assertEquals(date, WikibaseRepository.inputDateFormat().format(argumentDate.getValue()));
        assertEquals(contJson, argumentJson.getValue());
    }

    /**
     * Check that delete is processed.
     * @throws RetryableException
     */
    @Test
    @SuppressWarnings("unchecked")
    public void delete() throws RetryableException {
        // Use old date to remove backoff
        Date startTime = DateUtils.addDays(new Date(), -10);
        // Build a result from wikibase with duplicate recent changes
        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);
        String date = WikibaseRepository.inputDateFormat().format(new Date());
        JSONObject rc = new JSONObject();
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q424242");
        rc.put("timestamp", date);
        rc.put("revid", Long.valueOf(0)); // 0 means delete
        rc.put("rcid", 42L);
        rc.put("type", "log");
        recentChanges.add(rc);

        rc = new JSONObject();
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q424242");
        rc.put("timestamp", date);
        rc.put("revid", 7L);
        rc.put("rcid", 45L);
        rc.put("type", "edit");
        recentChanges.add(rc);

        firstBatchReturns(startTime, result);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        List<Change> changes = batch.changes();
        assertThat(changes, hasSize(1));
        assertEquals(changes.get(0).entityId(), "Q424242");
        assertEquals(changes.get(0).rcid(), 42L);
        assertEquals(changes.get(0).revision(), -1L);
    }

    /**
     * Check that recent requests use backoff.
     * @throws RetryableException
     */
    @Test
    @SuppressWarnings("unchecked")
    public void backoffTime() throws RetryableException {
        Date startTime = new Date();
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);

        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);

        Date nextStartTime = DateUtils.addSeconds(startTime, 20);
        String date = WikibaseRepository.inputDateFormat().format(nextStartTime);
        JSONObject rc = new JSONObject();
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q424242");
        rc.put("timestamp", date);
        rc.put("revid", 42L);
        rc.put("rcid", 42L);
        rc.put("type", "edit");
        recentChanges.add(rc);

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);
        when(repository.fetchRecentChangesByTime(argument.capture(), eq(batchSize))).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);
        when(repository.isValidEntity(any(String.class))).thenReturn(true);

        Batch batch = poller.firstBatch();

        // Ensure we backed off at least 7 seconds but no more than 20
        assertThat(argument.getValue(), lessThan(DateUtils.addSeconds(startTime, -7)));
        assertThat(argument.getValue(), greaterThan(DateUtils.addSeconds(startTime, -20)));

        // Verify that backoff still works on the second call
        batch = poller.nextBatch(batch);
        assertNotNull(batch); // verify we're still using fetchRecentChangesByTime
        assertThat(argument.getValue(), lessThan(DateUtils.addSeconds(nextStartTime, -7)));
        assertThat(argument.getValue(), greaterThan(DateUtils.addSeconds(nextStartTime, -20)));

    }

    /**
     * Verify that no backoff happens for old changes.
     * @throws RetryableException
     */
    @SuppressWarnings("unchecked")
    public void noBackoffForOld() throws RetryableException {
        Date startTime = DateUtils.addDays(new Date(), -1);
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, 10);

        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);
        when(repository.fetchRecentChanges(argument.capture(), any(JSONObject.class), eq(batchSize))).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);
        when(repository.isValidEntity(any(String.class))).thenReturn(true);
        Batch batch = poller.firstBatch();

        assertEquals(argument.getValue(), startTime);
    }

    /**
     * Backoff overflow check,
     * Check that if we're backing off but find no new changes then time is advanced.
     * @throws RetryableException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void backoffOverflow() throws RetryableException {
        Date startTime = new Date();
        batchSize = 1;
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);

        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);

        String date = WikibaseRepository.inputDateFormat().format(startTime);
        JSONObject rc = new JSONObject();
        rc.put("ns", Long.valueOf(0));
        rc.put("title", "Q424242");
        rc.put("timestamp", date);
        rc.put("revid", 42L);
        rc.put("rcid", 42L);
        rc.put("type", "edit");
        recentChanges.add(rc);

        firstBatchReturns(startTime, result);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(1));
        assertEquals(startTime, batch.leftOffDate());

        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(0));
        assertThat(startTime, lessThan(batch.leftOffDate()));
        assertEquals(DateUtils.addSeconds(startTime, 1), batch.leftOffDate());
    }

    @Before
    public void setupMocks() {
        repository = mock(WikibaseRepository.class);
    }
}
