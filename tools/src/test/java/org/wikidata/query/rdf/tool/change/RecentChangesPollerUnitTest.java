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
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.OUTPUT_DATE_FORMATTER;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

@SuppressWarnings({"unchecked", "boxing"})
public class RecentChangesPollerUnitTest {
    private WikibaseRepository repository;

    private int batchSize = 10;

    /**
     * Mock return of the first batch call to repository.
     * @param startTime
     * @param result
     * @throws RetryableException
     */
    private void firstBatchReturns(Instant startTime, RecentChangeResponse result) throws RetryableException {
        when(repository.fetchRecentChangesByTime(any(Instant.class), eq(batchSize))).thenCallRealMethod();
        when(repository.fetchRecentChanges(any(Instant.class), eq(null), eq(batchSize))).thenReturn(result);
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
        Instant startTime = Instant.now();
        // Build a result from wikibase with duplicate recent changes
        List<RecentChange> recentChanges = new ArrayList<>();
        // 20 entries with 10 total Q ids
        for (long i = 0; i < 20; i++) {
            RecentChange rc = new RecentChange(
                    0L, "Q" + (i / 2), Instant.now(), i, i, "edit");
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
        Instant startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        int batchSize = 10;

        Instant revDate = startTime.plusSeconds(20);

        String error = null;
        Continue aContinue = new Continue(
                OUTPUT_DATE_FORMATTER.format(revDate) + "|8",
                "-||");
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q666", revDate, 1L, 1L, "edit"));
        recentChanges.add(new RecentChange(0L, "Q667", revDate, 7L, 7L, "edit"));
        Query query = new Query(recentChanges);

        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        String date = revDate.toString();

        firstBatchReturns(startTime, result);

        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);
        Batch batch = poller.firstBatch();
        assertThat(batch.changes(), hasSize(2));
        assertEquals(7, batch.changes().get(1).rcid());
        assertEquals(date, batch.leftOffDate().toString());
        assertEquals(aContinue, batch.getLastContinue());

        ArgumentCaptor<Instant> argumentDate = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Continue> continueCaptor = ArgumentCaptor.forClass(Continue.class);

        recentChanges.clear();
        when(repository.fetchRecentChanges(argumentDate.capture(), continueCaptor.capture(), eq(batchSize))).thenReturn(result);
        // check that poller passes the continue object to the next batch
        batch = poller.nextBatch(batch);
        assertThat(batch.changes(), hasSize(0));
        assertEquals(date, argumentDate.getValue().toString());
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
        Instant startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        // Build a result from wikibase with duplicate recent changes
        String error = null;
        Continue aContinue = null;
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q424242", Instant.now(), 0L, 42L, "log"));
        recentChanges.add(new RecentChange(0L, "Q424242", Instant.now(), 7L, 45L, "edit"));
        Query query = new Query(recentChanges);
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

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
        Instant startTime = Instant.now();
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, batchSize);

        Instant nextStartTime = startTime.plusSeconds(20);

        String error = null;
        Continue aContinue = null;
        List<RecentChange> recentChanges = new ArrayList<>();
        recentChanges.add(new RecentChange(0L, "Q424242", nextStartTime, 42L, 42L, "edit"));
        Query query = new Query(recentChanges);
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        ArgumentCaptor<Instant> argument = ArgumentCaptor.forClass(Instant.class);
        when(repository.fetchRecentChangesByTime(argument.capture(), eq(batchSize))).thenReturn(result);
        when(repository.isEntityNamespace(0)).thenReturn(true);
        when(repository.isValidEntity(any(String.class))).thenReturn(true);

        Batch batch = poller.firstBatch();

        // Ensure we backed off at least 7 seconds but no more than 20
        assertThat(argument.getValue(), lessThan(startTime.minusSeconds(7)));
        assertThat(argument.getValue(), greaterThan(startTime.minusSeconds(20)));

        // Verify that backoff still works on the second call
        batch = poller.nextBatch(batch);
        assertNotNull(batch); // verify we're still using fetchRecentChangesByTime
        assertThat(argument.getValue(), lessThan(nextStartTime.minusSeconds(7)));
        assertThat(argument.getValue(), greaterThan(nextStartTime.minusSeconds(20)));

    }

    /**
     * Verify that no backoff happens for old changes.
     * @throws RetryableException
     */
    public void noBackoffForOld() throws RetryableException {
        Instant startTime = Instant.now().minus(1, ChronoUnit.DAYS);
        RecentChangesPoller poller = new RecentChangesPoller(repository, startTime, 10);

        String error = null;
        Continue aContinue = null;
        Query query = new Query(emptyList());
        RecentChangeResponse result = new RecentChangeResponse(error, aContinue, query);

        ArgumentCaptor<Instant> argument = ArgumentCaptor.forClass(Instant.class);
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
    @Test
    public void backoffOverflow() throws RetryableException {
        Instant startTime = Instant.now();
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
        assertEquals(startTime.plusSeconds(1), batch.leftOffDate());
    }

    @Before
    public void setupMocks() {
        repository = mock(WikibaseRepository.class);
    }
}
