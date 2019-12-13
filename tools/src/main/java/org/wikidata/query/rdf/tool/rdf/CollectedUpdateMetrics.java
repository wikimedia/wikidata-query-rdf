package org.wikidata.query.rdf.tool.rdf;

import static org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics.ZERO_METRICS;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics;

public class CollectedUpdateMetrics {
    private Map<MultiSyncStep, UpdateMetrics> updateMetricsMap = new EnumMap<>(MultiSyncStep.class);

    private int commitTotalElapsed;
    private int mutationCount;

    public void merge(MultiSyncStep step, UpdateMetrics updateMetrics) {
        updateMetricsMap.merge(step, updateMetrics, UpdateMetrics::add);
    }

    public void merge(CollectedUpdateMetrics collectedUpdateMetrics) {
        for (MultiSyncStep step : MultiSyncStep.values()) {
            this.merge(step, collectedUpdateMetrics.getMetrics(step));
        }
        this.mutationCount += collectedUpdateMetrics.mutationCount;
        this.commitTotalElapsed += collectedUpdateMetrics.commitTotalElapsed;
    }

    public int getMutationCount() {
        return mutationCount;
    }

    public Set<MultiSyncStep> definedMetrics() {
        return updateMetricsMap.keySet();
    }

    public UpdateMetrics getMetrics(MultiSyncStep step) {
        return updateMetricsMap.getOrDefault(step, ZERO_METRICS);
    }

    public static CollectedUpdateMetrics getMutationCountOnlyMetrics(int count) {
        CollectedUpdateMetrics collectedUpdateMetrics = new CollectedUpdateMetrics();
        collectedUpdateMetrics.setMutationCount(count);
        return collectedUpdateMetrics;
    }

    public int getCommitTotalElapsed() {
        return commitTotalElapsed;
    }

    public void setCommitTotalElapsed(int commitTotalElapsed) {
        this.commitTotalElapsed = commitTotalElapsed;
    }

    public void setMutationCount(int mutationCount) {
        this.mutationCount = mutationCount;
    }
}
