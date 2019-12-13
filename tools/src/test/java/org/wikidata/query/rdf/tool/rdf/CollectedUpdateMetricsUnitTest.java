package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.ADD_TIMESTAMPS;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.INSERT_NEW_DATA;

import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetrics;

public class CollectedUpdateMetricsUnitTest {


    private CollectedUpdateMetrics collectedUpdateMetrics;

    @Before
    public void setUp() {
        collectedUpdateMetrics = new CollectedUpdateMetrics();
    }

    @Test
    public void shouldReturnDefinedMetricsAndTotalMutationCount() {
        collectedUpdateMetrics.merge(INSERT_NEW_DATA, createSampleUpdateMetrics());
        collectedUpdateMetrics.merge(ADD_TIMESTAMPS, createSampleUpdateMetrics());
        collectedUpdateMetrics.setMutationCount(10);

        assertThat(collectedUpdateMetrics.definedMetrics()).containsExactly(INSERT_NEW_DATA, ADD_TIMESTAMPS);
        assertThat(collectedUpdateMetrics.getMutationCount()).isEqualTo(10);
    }

    @Test
    public void shouldSummarizeSameStepMetrics() {
        collectedUpdateMetrics.merge(INSERT_NEW_DATA, createSampleUpdateMetrics());
        collectedUpdateMetrics.merge(INSERT_NEW_DATA, createSampleUpdateMetrics());
        collectedUpdateMetrics.setMutationCount(10);

        assertThat(collectedUpdateMetrics.definedMetrics()).containsExactly(INSERT_NEW_DATA);
        assertThat(collectedUpdateMetrics.getMutationCount()).isEqualTo(10);
        UpdateMetrics expected = UpdateMetrics.builder()
                .elapsed(60)
                .totalElapsed(6)
                .whereClause(6)
                .insertClause(2)
                .deleteClause(4)
                .batchResolve(2)
                .connFlush(10)
                .build();

        assertThat(collectedUpdateMetrics.getMetrics(INSERT_NEW_DATA)).isEqualTo(expected);
    }


    private UpdateMetrics createSampleUpdateMetrics() {
        return UpdateMetrics.builder()
                .elapsed(30)
                .totalElapsed(3)
                .whereClause(3)
                .insertClause(1)
                .deleteClause(2)
                .batchResolve(1)
                .connFlush(5)
                .build();
    }
}
