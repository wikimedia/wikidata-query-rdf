package org.wikidata.query.rdf.tool;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.SortedMap;

import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;
import org.wikidata.query.rdf.tool.rdf.MultiSyncStep;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

public class UpdaterMetricsRepositoryUnitTest {

    private MetricRegistry metricRegistry;
    private UpdaterMetricsRepository updaterMetricsRepository;

    @Before
    public void setUp() {
        metricRegistry = new MetricRegistry();
        updaterMetricsRepository = new UpdaterMetricsRepository(metricRegistry);
    }

    @Test
    public void testBasicMetrics() {
        updaterMetricsRepository.markSkipAhead();
        updaterMetricsRepository.incNoopedChangesByRevisionCheck(2);
        updaterMetricsRepository.markBatchProgress(5);

        updaterMetricsRepository.incDeferredChanges();
        updaterMetricsRepository.incDeferredChanges();

        Meter updatesSkip = metricRegistry.meter("updates-skip");
        Counter noops = metricRegistry.counter("noop-by-revision-check");
        Meter batchProgress = metricRegistry.meter("batch-progress");
        Counter delayedChanges = metricRegistry.counter("updates-deferred-changes");

        assertThat(updatesSkip.getCount()).isEqualTo(1);
        assertThat(noops.getCount()).isEqualTo(2);
        assertThat(batchProgress.getCount()).isEqualTo(5);
        assertThat(delayedChanges.getCount()).isEqualTo(2);
    }


    @Test
    public void testBlazegraphMetrics() {
        CollectedUpdateMetrics collectedUpdateMetrics = new CollectedUpdateMetrics();
        collectedUpdateMetrics.setMutationCount(10);
        collectedUpdateMetrics.setCommitTotalElapsed(45);
        for (MultiSyncStep step : MultiSyncStep.values()) {
            collectedUpdateMetrics.merge(step, createSampleUpdateMetrics());
        }

        updaterMetricsRepository.updateChanges(() -> collectedUpdateMetrics, 10);

        for (MultiSyncStep step : MultiSyncStep.values()) {
            String base = step.getMetricBaseName();
            SortedMap<String, Counter> counters = metricRegistry.getCounters(MetricFilter.startsWith(base));
            assertThat(counters.size()).isEqualTo(MultiSyncStep.values().length);

            assertThat(counters.get(base + "elapsed").getCount()).isEqualTo(5);
            assertThat(counters.get(base + "batch-resolve").getCount()).isEqualTo(0);
            assertThat(counters.get(base + "delete-clause").getCount()).isEqualTo(0);
            assertThat(counters.get(base + "insert-clause").getCount()).isEqualTo(1);
            assertThat(counters.get(base + "total-elapsed").getCount()).isEqualTo(4);
            assertThat(counters.get(base + "conn-flush").getCount()).isEqualTo(1);
            assertThat(counters.get(base + "where-clause").getCount()).isEqualTo(3);
        }

        assertThat(metricRegistry.counter("rdf-repository-imported-triples").getCount()).isEqualTo(10);
        assertThat(metricRegistry.counter("rdf-repository-imported-changes").getCount()).isEqualTo(10);
        assertThat(metricRegistry.counter("blazegraph-commit-total-elapsed").getCount()).isEqualTo(45);
    }




    private UpdateMetrics createSampleUpdateMetrics() {
        return UpdateMetrics.builder()
                .elapsed(5)
                .batchResolve(0)
                .deleteClause(0)
                .insertClause(1)
                .totalElapsed(4)
                .connFlush(1)
                .whereClause(3)
                .build();
    }
}
