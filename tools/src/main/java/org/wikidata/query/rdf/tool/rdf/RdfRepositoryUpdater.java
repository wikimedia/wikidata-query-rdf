package org.wikidata.query.rdf.tool.rdf;

import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import lombok.SneakyThrows;
import lombok.Value;

public class RdfRepositoryUpdater implements AutoCloseable {

    /**
     * Max POST form content size.
     * Should be in sync with Jetty org.eclipse.jetty.server.Request.maxFormContentSize setting.
     * Production default is 200M, see runBlazegraph.sh file.
     * This is only used for reconciliation so hopefully such limits won't be reached too often.
     */
    private static final long MAX_FORM_CONTENT_SIZE = Long.getLong("RDFRepositoryMaxPostSize", 200_000_000);

    private static final String TIMEOUT_PROPERTY = RdfRepositoryUpdater.class + ".timeout";

    private final RdfClient client;
    private final UrisScheme uris;

    /**
     * Concise struct data type to hold an update template and its argument name (single arg template).
     */
    @Value
    private static class UpdateDataTemplate {
        String template;
        String dataArgument;
    }

    private static final UpdateDataTemplate INSERT_DATA = new UpdateDataTemplate(loadBody("insertData"), "insertStatements");
    private static final UpdateDataTemplate DELETE_DATA = new UpdateDataTemplate(loadBody("deleteData"), "deleteStatements");
    private static final String DELETE_ENTITY = loadBody("deleteEntity");
    private static final String UPDATE_EVENT_TIME = loadBody("avgEventTime");
    private final RdfRepository rdfRepository;

    public RdfRepositoryUpdater(RdfClient client, UrisScheme uris) {
        this(client, uris, MAX_FORM_CONTENT_SIZE);
    }

    public RdfRepositoryUpdater(RdfClient client, UrisScheme uris, long maxPostSize) {
        this.client = client;
        this.uris = uris;
        this.rdfRepository = new RdfRepository(uris, client, maxPostSize);
    }

    private static String loadBody(String name) {
        return Utils.loadBody(name, RdfRepositoryUpdater.class);
    }

    public RDFPatchResult applyPatch(ConsumerPatch patch, @Nullable Instant avgEventTime) {
        StringBuilder sb = new StringBuilder();
        int expectedMutations = appendUpdateDataQuery(sb, patch.getRemoved(), DELETE_DATA);
        expectedMutations += appendUpdateDataQuery(sb, patch.getAdded(), INSERT_DATA);
        if (expectedMutations == 0 && patch.getEntityIdsToDelete().isEmpty() && patch.getReconciliations().isEmpty()) {
            throw new IllegalArgumentException("Empty patch given");
        }

        int actualMutations = 0;
        if (expectedMutations > 0) {
            actualMutations = client.update(sb.toString());
        }
        sb.setLength(0);
        int expectedSharedEltMutations = appendUpdateDataQuery(sb, patch.getLinkedSharedElements(), INSERT_DATA);

        // We expect to change the event time
        expectedSharedEltMutations++;
        int actualSharedEltMutations = 0;
        if (avgEventTime != null) {
            // Only update event time if we have something
            actualSharedEltMutations = updateEventTime(avgEventTime, sb, actualSharedEltMutations);
        }

        int deleteMutations = 0;
        if (patch.getEntityIdsToDelete().size() > 0) {
           deleteMutations = deleteEntities(patch.getEntityIdsToDelete());
        }
        int reconciliationMutation = 0;
        if (patch.getReconciliations().size() > 0) {
            reconciliationMutation = reconciliationMutation(patch.getReconciliations());
        }

        return new RDFPatchResult(expectedMutations, actualMutations, expectedSharedEltMutations,
                actualSharedEltMutations, deleteMutations, reconciliationMutation);
    }

    private int appendUpdateDataQuery(StringBuilder sb, Collection<Statement> data, UpdateDataTemplate template) {
        if (!data.isEmpty()) {
            UpdateBuilder builder = new UpdateBuilder(template.template);
            builder.bindStatements(template.dataArgument, data);
            sb.append(builder);
        }
        return data.size();
    }

    private int updateEventTime(Instant avgEventTime, StringBuilder sb, int actualSharedEltMutations) {
        // Mimic the old updater by maintaining the avg event time found in this batch
        UpdateBuilder b = new UpdateBuilder(UPDATE_EVENT_TIME);
        b.bindUri("root", uris.root());
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        b.bindValue("date", avgEventTime);
        sb.append(b);
        if (sb.length() > 0) {
            actualSharedEltMutations = client.update(sb.toString());
        }
        return actualSharedEltMutations;
    }

    private int reconciliationMutation(Map<String, Collection<Statement>> reconciliations) {
        List<RdfRepository.EntityData> data = reconciliations.entrySet().stream()
                .map(e -> new RdfRepository.EntityData(e.getKey(), e.getValue(), emptyList(), emptyList()))
                .collect(toList());
        CollectedUpdateMetrics collectedUpdateMetrics = this.rdfRepository.syncFromEntityData(new RdfRepository.EntityDataUpdateBatch(false, data), false);
        return collectedUpdateMetrics.getMutationCount();
    }

    private int deleteEntities(List<String> entityIds) {
        UpdateBuilder builder = new UpdateBuilder(DELETE_ENTITY);
        builder.bindEntityIds("entityList", entityIds, uris);
        builder.bind("uris.statement", uris.statement());
        builder.bindUri("schema:about", SchemaDotOrg.ABOUT);
        return client.update(builder.toString());
    }

    public static Duration getRdfClientTimeout() {
        int timeout = parseInt(System.getProperty(TIMEOUT_PROPERTY, "-1"));
        return Duration.of(timeout, ChronoUnit.SECONDS);
    }

    @SneakyThrows
    public void close() {
        client.httpClient.stop();
    }
}
