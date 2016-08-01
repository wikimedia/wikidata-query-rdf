package org.wikidata.query.rdf.tool.change;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
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

    @Test
    @SuppressWarnings("unchecked")
    public void dedups() throws RetryableException {
        Date startTime = new Date();
        int batchSize = 10;
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
        when(repository.fetchRecentChangesBackoff(startTime, batchSize, true)).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);

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

        Date revDate = new Date();
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

        when(repository.fetchRecentChangesBackoff(startTime, batchSize, true)).thenReturn(result);
        when(repository.getContinueObject((Change)any())).thenReturn(contJson);
        when(repository.isEntityNamespace(0)).thenReturn(true);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(2));
        assertEquals(7, batch.changes().get(1).rcid());

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);

        recentChanges.clear();
        when(repository.fetchRecentChangesBackoff(argument.capture(), eq(batchSize), eq(true))).thenReturn(result);
        // check that poller passes the continue object to the next batch
        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(0));
        assertEquals(date, WikibaseRepository.inputDateFormat().format(argument.getValue()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void delete() throws RetryableException {
        Date startTime = new Date();
        int batchSize = 10;
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
        rc.put("revid", Long.valueOf(0));
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

        when(repository.fetchRecentChangesBackoff(startTime, batchSize, true)).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        List<Change> changes = batch.changes();
        assertThat(changes, hasSize(1));
        assertEquals(changes.get(0).entityId(), "Q424242");
        assertEquals(changes.get(0).rcid(), 42L);
        assertEquals(changes.get(0).revision(), -1L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void backoffTime() throws RetryableException {
        Date startTime = new Date();
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, 10);

        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);
        when(repository.fetchRecentChanges((Date)any(), anyInt())).thenReturn(result);
        when(repository.fetchRecentChangesBackoff((Date)any(), anyInt(), anyBoolean())).thenCallRealMethod();
        Batch batch = poller.firstBatch();

        verify(repository).fetchRecentChanges(argument.capture(), eq(10));
        // Ensure we backed off at least 10 seconds but no more than 30
        assertThat(argument.getValue(), lessThan(DateUtils.addSeconds(startTime, -7)));
        assertThat(argument.getValue(), greaterThan(DateUtils.addSeconds(startTime, -30)));
    }

    @SuppressWarnings("unchecked")
    public void backoffTimeOld() throws RetryableException {
        Date startTime = DateUtils.addDays(new Date(), -1);
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, 10);

        JSONObject result = new JSONObject();
        JSONObject query = new JSONObject();
        result.put("query", query);
        JSONArray recentChanges = new JSONArray();
        query.put("recentchanges", recentChanges);

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);
        when(repository.fetchRecentChanges((Date)any(), anyInt())).thenReturn(result);
        when(repository.fetchRecentChangesBackoff((Date)any(), anyInt(), anyBoolean())).thenCallRealMethod();
        Batch batch = poller.firstBatch();

        verify(repository).fetchRecentChanges(argument.capture(), eq(10));
        // Ensure we backed off no more than one second
        assertThat(argument.getValue(), lessThan(startTime));
        assertThat(argument.getValue(), greaterThan(DateUtils.addSeconds(startTime, -2)));
    }


    @Test
    public void backoffArg() throws RetryableException {
        Date startTime = new Date();
        JSONObject result = new JSONObject();

        when(repository.fetchRecentChanges((Date)any(), eq(10))).thenReturn(result);
        when(repository.fetchRecentChangesBackoff((Date)any(), anyInt(), anyBoolean())).thenCallRealMethod();
        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);

        repository.fetchRecentChangesBackoff(startTime, 10, false);
        verify(repository).fetchRecentChanges(argument.capture(), eq(10));
        assertEquals(startTime, argument.getValue());
    }

    @Before
    public void setupMocks() {
        repository = mock(WikibaseRepository.class);
    }
}
