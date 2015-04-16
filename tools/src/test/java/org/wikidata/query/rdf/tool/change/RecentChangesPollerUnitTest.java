package org.wikidata.query.rdf.tool.change;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
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
            recentChanges.add(rc);
        }
        when(repository.fetchRecentChanges(startTime, null, batchSize)).thenReturn(result);

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

    @Before
    public void setupMocks() {
        repository = mock(WikibaseRepository.class);
    }
}
