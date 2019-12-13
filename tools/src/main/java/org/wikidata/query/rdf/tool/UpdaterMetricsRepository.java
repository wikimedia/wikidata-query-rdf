package org.wikidata.query.rdf.tool;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import org.wikidata.query.rdf.common.TimerCounter;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;
import org.wikidata.query.rdf.tool.rdf.MultiSyncStep;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class UpdaterMetricsRepository {

    /**
     * Meter for the raw number of updates synced.
     */
    private final Meter updatesMeter;
    /**
     * Meter measuring in a batch specific unit. For the RecentChangesPoller its
     * milliseconds, for the IdChangeSource its ids.
     */
    private final Meter batchAdvanced;
    /**
     * Measure how many updates skipped ahead of their change revisions.
     */
    private final Meter skipAheadMeter;

    private final TimerCounter wikibaseDataFetchTime;
    private final TimerCounter rdfRepositoryImportTime;
    private final TimerCounter rdfRepositoryFetchTime;
    private final Counter importedChanged;
    private final Counter noopedChangesByRevisionCheck;
    private final Counter importedTriples;

    private final Map<MultiSyncStep, BlazeGraphMetrics> blazeGraphMetrics = new EnumMap<>(MultiSyncStep.class);
    private final Counter blazeGraphCommitTotalElapsed;

    public UpdaterMetricsRepository(MetricRegistry metricRegistry) {
        this.updatesMeter = metricRegistry.meter("updates");
        this.batchAdvanced = metricRegistry.meter("batch-progress");
        this.skipAheadMeter = metricRegistry.meter("updates-skip");
        this.wikibaseDataFetchTime = TimerCounter.counter(metricRegistry.counter("wikibase-data-fetch-time-cnt"));
        this.rdfRepositoryImportTime = TimerCounter.counter(metricRegistry.counter("rdf-repository-import-time-cnt"));
        this.rdfRepositoryFetchTime = TimerCounter.counter(metricRegistry.counter("rdf-repository-fetch-time-cnt"));
        this.noopedChangesByRevisionCheck = metricRegistry.counter("noop-by-revision-check");
        this.importedChanged = metricRegistry.counter("rdf-repository-imported-changes");
        this.importedTriples = metricRegistry.counter("rdf-repository-imported-triples");
        this.blazeGraphCommitTotalElapsed = metricRegistry.counter("blazegraph-commit-total-elapsed");

        //update metrics from blazegraph
        for (MultiSyncStep step : MultiSyncStep.values()) {
            blazeGraphMetrics.put(step, new BlazeGraphMetrics(step.getMetricBaseName(), metricRegistry));
        }
    }

    public void markBatchProgress(long n) {
        batchAdvanced.mark(n);
    }

    public String reportUpdatesMeter() {
        return meterReport(updatesMeter);
    }

    public String reportBatchProgress() {
        return meterReport(batchAdvanced);
    }

    public <T> T timeRdfFetch(Supplier<T> supplier) {
        return rdfRepositoryFetchTime.time(supplier);
    }

    public <T, E extends Exception> T timeWikibaseDataFetch(TimerCounter.CheckedCallable<T, E> supplier) throws E {
        return wikibaseDataFetchTime.timeCheckedCallable(supplier);
    }

    public void incNoopedChangesByRevisionCheck(long n) {
        noopedChangesByRevisionCheck.inc(n);
    }

    public void updateChanges(Supplier<CollectedUpdateMetrics> supplier, long changesCount) {
        CollectedUpdateMetrics updateMetrics = rdfRepositoryImportTime.time(supplier);
        updatesMeter.mark(changesCount);
        importedChanged.inc(changesCount);
        updateMetricsFrom(updateMetrics);
    }

    public void markSkipAhead() {
        skipAheadMeter.mark();
    }

    private void updateMetricsFrom(CollectedUpdateMetrics updateMetrics) {
        importedTriples.inc(updateMetrics.getMutationCount());
        blazeGraphCommitTotalElapsed.inc(updateMetrics.getCommitTotalElapsed());

        for (MultiSyncStep step : updateMetrics.definedMetrics()) {
            blazeGraphMetrics.get(step).updateMetrics(updateMetrics.getMetrics(step));
        }
    }

    /**
     * Turn a Meter into a load average style report.
     */
    private String meterReport(Meter meter) {
        return String.format(Locale.ROOT, "(%.1f, %.1f, %.1f)", meter.getOneMinuteRate(), meter.getFiveMinuteRate(),
                meter.getFifteenMinuteRate());
    }

    private static class BlazeGraphMetrics {
        private final Counter totalElapsed;
        private final Counter insertClause;
        private final Counter deleteClause;
        private final Counter whereClause;
        private final Counter batchResolve;
        private final Counter connFlush;
        private final Counter elapsed;

         BlazeGraphMetrics(String baseName, MetricRegistry metricRegistry) {
            totalElapsed = metricRegistry.counter(baseName + "total-elapsed");
            insertClause = metricRegistry.counter(baseName + "insert-clause");
            deleteClause = metricRegistry.counter(baseName + "delete-clause");
            whereClause = metricRegistry.counter(baseName + "where-clause");
            batchResolve = metricRegistry.counter(baseName + "batch-resolve");
            connFlush = metricRegistry.counter(baseName + "conn-flush");
            elapsed = metricRegistry.counter(baseName + "elapsed");
        }


        void updateMetrics(UpdateMetrics metrics) {
            updateOnNonNull(metrics.getTotalElapsed(), totalElapsed);

            updateOnNonNull(metrics.getInsertClause(), insertClause);
            updateOnNonNull(metrics.getDeleteClause(), deleteClause);
            updateOnNonNull(metrics.getWhereClause(), whereClause);
            updateOnNonNull(metrics.getBatchResolve(), batchResolve);
            updateOnNonNull(metrics.getConnFlush(), connFlush);
            updateOnNonNull(metrics.getElapsed(), elapsed);
        }

        private void updateOnNonNull(Integer value, Counter counter) {
             if (value != null) counter.inc(value);
        }
    }
}
