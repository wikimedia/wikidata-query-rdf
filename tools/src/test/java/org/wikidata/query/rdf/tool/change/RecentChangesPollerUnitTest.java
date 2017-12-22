package org.wikidata.query.rdf.tool.change;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.outputDateFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.wikidata.query.rdf.tool.change.RecentChangesPoller.Batch;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse;
import org.wikidata.query.rdf.tool.wikibase.Continue;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.Query;
import org.wikidata.query.rdf.tool.wikibase.RecentChangeResponse.RecentChange;
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
    private void firstBatchReturns(Date startTime, RecentChangeResponse result) throws RetryableException {
        when(repository.fetchRecentChangesByTime(any(Date.class), eq(batchSize))).thenCallRealMethod();
        when(repository.fetchRecentChanges(any(Date.class), eq(null), eq(batchSize))).thenReturn(result);
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
        List<RecentChange> recentChanges = new ArrayList<>();
        // 20 entries with 10 total Q ids
        for (long i = 0; i < 20; i++) {
            RecentChange rc = new RecentChange(
                    0L, "Q" + (i / 2), new Date(), i, i, "edit");
            recentChanges.add(rc);
        }
        Query query = new Query(recentChanges);
        String error = null;
        Continue aContinue = null;
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        firstBatchReturns(startTime, result);
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();

        assertThat(batch.changes(), hasSize(10));
        List<Change> changes = new ArrayList<>(batch.changes());
        Collections.sort(changes, Comparator.comparing(Change::entityId));
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

        Date revDate = DateUtils.addSeconds(startTime, 20);

        String error = null;
        Continue aContinue = new Continue(
                outputDateFormat().format(revDate) + "|8",
                "-||");
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q666", revDate, 1L, 1L, "edit"));
        recentChanges.add(new RecentChange(0L, "Q667", revDate, 7L, 7L, "edit"));
        Query query = new Query(recentChanges);

        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        String date = WikibaseRepository.inputDateFormat().format(revDate);

        firstBatchReturns(startTime, result);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(2));
        assertEquals(7, batch.changes().get(1).rcid());
        assertEquals(date, WikibaseRepository.inputDateFormat().format(batch.leftOffDate()));
        assertEquals(aContinue, batch.getLastContinue());

        ArgumentCaptor<Date> argumentDate = ArgumentCaptor.forClass(Date.class);
        ArgumentCaptor<Continue> continueCaptor = ArgumentCaptor.forClass(Continue.class);

        recentChanges.clear();
        when(repository.fetchRecentChanges(argumentDate.capture(), continueCaptor.capture(), eq(batchSize))).thenReturn(result);
        // check that poller passes the continue object to the next batch
        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(0));
        assertEquals(date, WikibaseRepository.inputDateFormat().format(argumentDate.getValue()));
        assertEquals(aContinue, continueCaptor.getValue());
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
        String error = null;
        Continue aContinue = null;
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q424242", new Date(), 0L, 42L, "log"));
        recentChanges.add(new RecentChange(0L, "Q424242", new Date(), 7L, 45L, "edit"));
        Query query = new Query(recentChanges);
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);
        String date = WikibaseRepository.inputDateFormat().format(new Date());

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

        Date nextStartTime = DateUtils.addSeconds(startTime, 20);
        String date = WikibaseRepository.inputDateFormat().format(nextStartTime);

        String error = null;
        Continue aContinue = null;
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q424242", nextStartTime, 42L, 42L, "edit"));
        Query query = new Query(recentChanges);
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

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

        String error = null;
        Continue aContinue = null;
        Query query = new Query(emptyList());
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);
        when(repository.fetchRecentChanges(argument.capture(), any(), eq(batchSize))).thenReturn(result);
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

        String error = null;
        Continue aContinue = null;
        ArrayList<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q424242", startTime, 42L, 42L, "edit"));
        Query query = new Query(recentChanges);
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

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
