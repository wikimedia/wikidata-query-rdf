package org.wikidata.query.rdf.tool.rdf;

import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import lombok.SneakyThrows;

public class RdfRepositoryUpdater {
    private final RdfClient client;

    private static final String INSERT_DATA = loadBody("insertData");
    private static final String DELETE_DATA = loadBody("deleteData");

    public RdfRepositoryUpdater(RdfClient client) {
        this.client = client;
    }

    private static String loadBody(String name) {
        return Utils.loadBody(name, RdfRepositoryUpdater.class);
    }

    public RDFPatchResult applyPatch(RDFPatch patch) {
        int expectedMutations = 0;
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
        if (expectedMutations == 0) {
            throw new IllegalArgumentException("Empty patch given");
        }
        int actualMutations = client.update(sb.toString());
        int expectedSharedEltMutations = 0;
        int actualSharedEltMutations = 0;
        if (!patch.getLinkedSharedElements().isEmpty()) {
            UpdateBuilder builder = new UpdateBuilder(INSERT_DATA);
            builder.bindStatements("insertStatements", patch.getLinkedSharedElements());
            expectedSharedEltMutations += patch.getLinkedSharedElements().size();
            actualSharedEltMutations = client.update(builder.toString());
        }

        return new RDFPatchResult(expectedMutations, actualMutations, expectedSharedEltMutations, actualSharedEltMutations);
    }

    @SneakyThrows
    public void close() {
        client.httpClient.stop();
    }
}
