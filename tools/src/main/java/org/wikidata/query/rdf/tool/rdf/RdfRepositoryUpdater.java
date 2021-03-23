package org.wikidata.query.rdf.tool.rdf;

import static java.lang.Integer.parseInt;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import lombok.SneakyThrows;

public class RdfRepositoryUpdater implements AutoCloseable {
    private static final String TIMEOUT_PROPERTY = RdfRepositoryUpdater.class + ".timeout";

    private final RdfClient client;
    private final UrisScheme uris;

    private static final String INSERT_DATA = loadBody("insertData");
    private static final String DELETE_DATA = loadBody("deleteData");
    private static final String DELETE_ENTITY = loadBody("deleteEntity");
    private static final String UPDATE_EVENT_TIME = loadBody("avgEventTime");

    public RdfRepositoryUpdater(RdfClient client, UrisScheme uris) {
        this.client = client;
        this.uris = uris;
    }

    private static String loadBody(String name) {
        return Utils.loadBody(name, RdfRepositoryUpdater.class);
    }

    public RDFPatchResult applyPatch(ConsumerPatch patch, Instant avgEventTime) {
        int expectedMutations = 0;
        int actualMutations = 0;
        StringBuilder sb = new StringBuilder();
        if (!patch.getRemoved().isEmpty()) {
            UpdateBuilder builder = new UpdateBuilder(DELETE_DATA);
            builder.bindStatements("deleteStatements", patch.getRemoved());
            expectedMutations += patch.getRemoved().size();
            sb.append(builder);
        }
        if (!patch.getAdded().isEmpty()) {
            UpdateBuilder builder = new UpdateBuilder(INSERT_DATA);
            builder.bindStatements("insertStatements", patch.getAdded());
            expectedMutations += patch.getAdded().size();
            sb.append(builder);
        }
        if (expectedMutations == 0 && patch.getEntityIdsToDelete().isEmpty()) {
            throw new IllegalArgumentException("Empty patch given");
        }
        if (expectedMutations > 0) {
            actualMutations = client.update(sb.toString());
        }
        int expectedSharedEltMutations = 0;
        int actualSharedEltMutations = 0;
        sb.setLength(0);
        if (!patch.getLinkedSharedElements().isEmpty()) {
            UpdateBuilder builder = new UpdateBuilder(INSERT_DATA);
            builder.bindStatements("insertStatements", patch.getLinkedSharedElements());
            expectedSharedEltMutations += patch.getLinkedSharedElements().size();
            sb.append(builder);
        }

        // We expect to change the event time
        expectedSharedEltMutations++;
        // Mimic the old updater by maintaining the avg event time found in this batch
        UpdateBuilder b = new UpdateBuilder(UPDATE_EVENT_TIME);
        b.bindUri("root", uris.root());
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        b.bindValue("date", avgEventTime);
        sb.append(b);
        if (sb.length() > 0) {
            actualSharedEltMutations = client.update(sb.toString());
        }

        int deleteMutations = 0;
        if (patch.getEntityIdsToDelete().size() > 0) {
           deleteMutations = deleteEntities(patch.getEntityIdsToDelete());
        }

        return new RDFPatchResult(expectedMutations, actualMutations, expectedSharedEltMutations, actualSharedEltMutations, deleteMutations);
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
