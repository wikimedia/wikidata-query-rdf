package org.wikidata.query.rdf.updater.consumer;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.TimerCounter;
import org.wikidata.query.rdf.tool.rdf.RDFPatchResult;
import org.wikidata.query.rdf.tool.rdf.RdfRepositoryUpdater;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

public class StreamingUpdaterConsumer implements Runnable {
    public static final Duration TIMEOUT = Duration.ofSeconds(3);
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final StreamConsumer consumer;
    private final RdfRepositoryUpdater repository;
    private final Counter divergencesCnt;
    private final Counter actualSharedMutationsCnt;
    private final Counter redundantSharedEltTriplesCnt;
    private final Counter mutationsCnt;
    private final Counter deleteMutationsCnt;
    private final Counter reconciliationMutationCnt;
    private final TimerCounter pollTimeCnt;
    private final TimerCounter rdfStoreTimeCnt;
    private final float inconsistencyWarningThreshold;

    private volatile boolean stop;

    public StreamingUpdaterConsumer(StreamConsumer consumer,
                                    RdfRepositoryUpdater repository,
                                    MetricRegistry registry,
                                    float inconsistencyWarningThreshold) {
        this.consumer = consumer;
        this.repository = repository;
        this.mutationsCnt = registry.counter("mutations");
        this.deleteMutationsCnt = registry.counter("delete-mutations");
        this.reconciliationMutationCnt = registry.counter("reconciliation-mutations");
        this.divergencesCnt = registry.counter("divergences");
        this.actualSharedMutationsCnt = registry.counter("shared-element-mutations");
        this.redundantSharedEltTriplesCnt = registry.counter("shared-element-redundant-mutations");
        this.pollTimeCnt = TimerCounter.counter(registry.counter("poll-time-cnt"));
        this.rdfStoreTimeCnt = TimerCounter.counter(registry.counter("rdf-store-time-cnt"));
        this.inconsistencyWarningThreshold = inconsistencyWarningThreshold;
    }

    public void run() {
        try (StreamConsumer consumer = this.consumer;
             RdfRepositoryUpdater repository = this.repository
        ) {
            // Use a custom flag instead of the interrupt flag because we want
            // to maximize our chances to properly cleanup our resources:
            // mainly close and commit pending kafka offsets and limit dup delivery
            // on restarts.
            // using interrupt() might mark some of the underlying IO resources
            // as unavailable preventing offsets to be committed.
            while (!stop) {
                StreamConsumer.Batch b = pollTimeCnt.time(() -> consumer.poll(TIMEOUT));
                if (b == null) {
                    continue;
                }
                RDFPatchResult result = rdfStoreTimeCnt.time(() -> repository.applyPatch(b.getPatch(), b.getAverageEventTime()));
                updateCounters(result);
                if (passInconsistencyThreshold(result, inconsistencyWarningThreshold)) {
                    logger.warn("Applied batch with too many inconsistencies. {} for {}.", result, b);
                }
                consumer.acknowledge();
            }
        }
    }

    /**
     * @return true if the pct of actual mutations minus expected ones is greater than the threshold
     */
    @VisibleForTesting
    public static boolean passInconsistencyThreshold(RDFPatchResult result, float inconsistencyWarningThreshold) {
        float expected = result.getExpectedMutations();
        float actual = result.getActualMutations();
        if (expected <= 0) {
            return false;
        }
        return (Math.abs(expected - actual) / expected) > inconsistencyWarningThreshold;
    }

    private void updateCounters(RDFPatchResult result) {
        mutationsCnt.inc(result.getActualMutations());
        divergencesCnt.inc(result.getExpectedMutations() - result.getActualMutations());
        actualSharedMutationsCnt.inc(result.getActualSharedElementsMutations());
        redundantSharedEltTriplesCnt.inc(result.getPossibleSharedElementMutations() - result.getActualSharedElementsMutations());
        deleteMutationsCnt.inc(result.getDeleteMutations());
        reconciliationMutationCnt.inc(result.getReconciliationMutations());
    }

    public void close() {
        this.stop = true;
    }
}
