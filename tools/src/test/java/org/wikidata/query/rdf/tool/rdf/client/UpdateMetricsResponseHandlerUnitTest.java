package org.wikidata.query.rdf.tool.rdf.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.ADD_TIMESTAMPS;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.CLEANUP_REFERENCES;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.CLEANUP_VALUES;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.CLEAR_OOD_SITE_LINKS;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.CLEAR_OOD_ST_ABOUT_ENT;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.CLEAR_OOD_ST_ABOUT_ST;
import static org.wikidata.query.rdf.tool.rdf.MultiSyncStep.INSERT_NEW_DATA;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;
import org.wikidata.query.rdf.tool.rdf.MultiSyncStep;

@RunWith(MockitoJUnitRunner.class)
public class UpdateMetricsResponseHandlerUnitTest {
    @Mock
    private ContentResponse response;

    private final UpdateMetricsResponseHandler responseHandler = new UpdateMetricsResponseHandler(true, true);

    private final Map<MultiSyncStep, List<Integer>> expectedMetrics = new HashMap<>();

    @Before
    public void initExpectedMetrics() {
        expectedMetrics.put(CLEAR_OOD_SITE_LINKS, asList(2, 1, 3, 8, 5, 6, 7));
        expectedMetrics.put(CLEAR_OOD_ST_ABOUT_ST, asList(5, 5, 43, 4, 2, 2, 1));
        expectedMetrics.put(CLEAR_OOD_ST_ABOUT_ENT, asList(3, 3, 23, 3, 6, 4, 2));
        expectedMetrics.put(INSERT_NEW_DATA, asList(1, 1, 13, 2, 1, 5, 3));
        expectedMetrics.put(ADD_TIMESTAMPS, asList(7, 5, 23, 1, 2, 3, 4));
        expectedMetrics.put(CLEANUP_REFERENCES, asList(3, 15, 3, 1, 12, 43, 14));
        expectedMetrics.put(CLEANUP_VALUES, asList(4, 2, 2, 12, 3, 3, 4));
    }

    @Test
    public void canParseUpdateMetrics() throws IOException {
        String content = loadCorrectResponseFromFile();
        setupResponse(content);

        CollectedUpdateMetrics collectedUpdateMetrics = responseHandler.parse(response);

        for (Map.Entry<MultiSyncStep, List<Integer>> entry : expectedMetrics.entrySet()) {
            UpdateMetrics updateMetrics = collectedUpdateMetrics.getMetrics(entry.getKey());
            List<Integer> metrics = entry.getValue();

            assertThat(updateMetrics.getTotalElapsed()).isEqualTo(metrics.get(0));
            assertThat(updateMetrics.getElapsed()).isEqualTo(metrics.get(1));
            assertThat(updateMetrics.getConnFlush()).isEqualTo(metrics.get(2));
            assertThat(updateMetrics.getBatchResolve()).isEqualTo(metrics.get(3));
            assertThat(updateMetrics.getWhereClause()).isEqualTo(metrics.get(4));
            assertThat(updateMetrics.getDeleteClause()).isEqualTo(metrics.get(5));
            assertThat(updateMetrics.getInsertClause()).isEqualTo(metrics.get(6));
        }

        assertThat(collectedUpdateMetrics.getMutationCount()).isEqualTo(4);
        assertThat(collectedUpdateMetrics.getCommitTotalElapsed()).isEqualTo(29);
    }

    @Test
    public void shouldNotThrowExceptionOnMalformedInput() throws IOException {
        String content = loadIncorrectResponseFromFile();
        setupResponse(content);

        CollectedUpdateMetrics collectedUpdateMetrics = responseHandler.parse(response);
        UpdateMetrics updateMetrics = collectedUpdateMetrics.getMetrics(CLEAR_OOD_SITE_LINKS);

        assertThat(collectedUpdateMetrics.getMutationCount()).isEqualTo(0);
        assertThat(collectedUpdateMetrics.getCommitTotalElapsed()).isEqualTo(0);

        assertThat(updateMetrics.getTotalElapsed()).isEqualTo(null);
        assertThat(updateMetrics.getElapsed()).isEqualTo(null);
        assertThat(updateMetrics.getConnFlush()).isEqualTo(null);
        assertThat(updateMetrics.getBatchResolve()).isEqualTo(null);
        assertThat(updateMetrics.getWhereClause()).isEqualTo(null);
        assertThat(updateMetrics.getDeleteClause()).isEqualTo(null);
        assertThat(updateMetrics.getInsertClause()).isEqualTo(null);
    }

    private String loadCorrectResponseFromFile() throws IOException {
        return  loadResponseFromFile("update-response.html");
    }

    private String loadIncorrectResponseFromFile() throws IOException {
        return  loadResponseFromFile("update-response-broken.html");
    }

    private String loadResponseFromFile(String resourceName) throws IOException {
        return  IOUtils.toString(getClass().getResourceAsStream(resourceName), UTF_8);
    }

    private void setupResponse(String content) {
        when(response.getContentAsString())
                .thenReturn(content);
    }
}
