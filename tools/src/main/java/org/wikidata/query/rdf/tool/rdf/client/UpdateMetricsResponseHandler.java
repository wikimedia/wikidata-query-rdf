package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;
import org.wikidata.query.rdf.tool.rdf.MultiSyncStep;

/**
 * Attempts to log update response information and return them but very likely only works
 * for Blazegraph.
 */
public class UpdateMetricsResponseHandler implements ResponseHandler<CollectedUpdateMetrics> {
    private static final Logger log = LoggerFactory.getLogger(UpdateMetricsResponseHandler.class);

    private static final String COMMIT_TOTAL_ELAPSED = "COMMIT: totalElapsed";
    private static final String MUTATION_COUNT = "mutationCount";

    private final List<MultiSyncStep> requiredSteps;

    public UpdateMetricsResponseHandler(boolean cleanUpRefs, boolean cleanUpValues, boolean withTimestamp) {
        requiredSteps = getRequiredSteps(cleanUpRefs, cleanUpValues, withTimestamp);
    }

    @Override
    public String acceptHeader() {
        return null;
    }

    @Override
    public CollectedUpdateMetrics parse(ContentResponse entity) throws IOException {

        String content = entity.getContentAsString();

        String[] lines = content.split("\\r?\\n");

        CollectedUpdateMetrics collectedUpdateMetrics = extractMetrics(lines);

        log.debug("Received metrics from update: {}", collectedUpdateMetrics);
        return collectedUpdateMetrics;
    }

    private CollectedUpdateMetrics extractMetrics(String[] lines) throws IOException {
        CollectedUpdateMetrics collectedUpdateMetrics = new CollectedUpdateMetrics();

        Iterator<String> lineIterator = getPerQueryResponses(lines);

        for (MultiSyncStep step : requiredSteps) {
            if (lineIterator.hasNext()) {
                collectedUpdateMetrics.merge(step, getUpdateMetrics(lineIterator.next()));
            } else {
                throw new IOException("Response didn't match the query!");
            }
        }

        Map<String, String> summaryMetrics = extractSummaryMetrics(lines);

        Integer commitTotalElapsed = asInt(summaryMetrics.get(COMMIT_TOTAL_ELAPSED));
        if (commitTotalElapsed != null) collectedUpdateMetrics.setCommitTotalElapsed(commitTotalElapsed);

        Integer mutationCount = asInt(summaryMetrics.get(MUTATION_COUNT));
        if (mutationCount == null) throw new IOException("Couldn't find the mutation count!");
        collectedUpdateMetrics.setMutationCount(mutationCount);

        return collectedUpdateMetrics;
    }

    private List<MultiSyncStep> getRequiredSteps(boolean cleanUpRefs, boolean cleanUpValues, boolean withTimestamp) {
        List<MultiSyncStep> requiredSteps = new ArrayList<>(Arrays.asList(MultiSyncStep.values()));
        if (!cleanUpRefs) requiredSteps.remove(MultiSyncStep.CLEANUP_REFERENCES);
        if (!cleanUpValues) requiredSteps.remove(MultiSyncStep.CLEANUP_VALUES);
        if (!withTimestamp) requiredSteps.remove(MultiSyncStep.ADD_TIMESTAMPS);
        return requiredSteps;
    }

    private Iterator<String> getPerQueryResponses(String[] lines) {
        return Stream.of(lines)
                    .filter(line -> line.contains("<p>totalElapsed"))
                    .iterator();
    }

    private Map<String, String> extractSummaryMetrics(String[] lines) {
        return Stream.of(lines)
                .filter(line -> line.contains(COMMIT_TOTAL_ELAPSED))
                .flatMap(this::extractStandardLinesMetrics)
                .collect(toMapCollector());
    }

    private UpdateMetrics getUpdateMetrics(String lines) {
        Stream<String[]> metrics = extractStandardLinesMetrics(lines);

        Map<String, String> stats = metrics.collect(toMapCollector());

        return UpdateMetrics.builder()
                .totalElapsed(asInt(stats.get("totalElapsed")))
                .insertClause(asInt(stats.get("insertClause")))
                .deleteClause(asInt(stats.get("deleteClause")))
                .whereClause(asInt(stats.get("whereClause")))
                .batchResolve(asInt(stats.get("batchResolve")))
                .connFlush(asInt(stats.get("connFlush")))
                .elapsed(asInt(stats.get("elapsed")))
                .build();
    }

    private Collector<String[], ?, Map<String, String>> toMapCollector() {
        return Collectors.toMap(stat -> stat[0], stat -> stat[1], (v1, v2) -> v2);
    }

    private Stream<String[]> extractStandardLinesMetrics(String line) {
        return Stream.of(line.split("[<>]"))
                .flatMap(l -> Stream.of(l.split(",")))
                .map(stat -> stat.split("="))
                .filter(stat -> stat.length == 2)
                .map(this::sanitize);
    }

    private Integer asInt(String rawIntMetric) {
        if (rawIntMetric == null) {
            return null;
        }

        try {
            return Integer.valueOf(rawIntMetric);
        } catch (NumberFormatException e) {
            log.error("Issue with the content - unparsable value: {}", rawIntMetric, e);
            return null;
        }
    }

    private String[] sanitize(String[] stat) {
        return new String[]{stat[0].trim(), stat[1].trim().replace("ms", "")};
    }
}
