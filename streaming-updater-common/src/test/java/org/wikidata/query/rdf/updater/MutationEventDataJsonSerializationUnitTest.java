package org.wikidata.query.rdf.updater;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;

import com.google.common.io.Resources;

public class MutationEventDataJsonSerializationUnitTest {
    private static final URL TEST_IMPORT_JSON_RESOURCE = MutationEventDataJsonSerializationUnitTest.class
            .getResource("MutationEventDataJsonSerializationUnitTest-testImport.json");
    private static final URL TEST_DIFF_JSON_RESOURCE = MutationEventDataJsonSerializationUnitTest.class
            .getResource("MutationEventDataJsonSerializationUnitTest-testDiff.json");
    private static final URL TEST_DELETE_JSON_RESOURCE = MutationEventDataJsonSerializationUnitTest.class
            .getResource("MutationEventDataJsonSerializationUnitTest-testDelete.json");
    private static final URL TEST_RECONCILE_JSON_RESOURCE = MutationEventDataJsonSerializationUnitTest.class
            .getResource("MutationEventDataJsonSerializationUnitTest-testReconcile.json");

    private final RDFChunkSerializer chunkSer;

    public MutationEventDataJsonSerializationUnitTest() {
        chunkSer = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
    }

    @Test
    public void testImport() throws IOException {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.fullImportEvent(() -> meta, "Q123", 123, eventTime, added, linkedData);
        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw.toString()).isEqualTo(Resources.toString(TEST_IMPORT_JSON_RESOURCE, UTF_8).replace("\n", ""));

        Object data = MapperUtils.getObjectMapper().readValue(TEST_IMPORT_JSON_RESOURCE, MutationEventData.class);
        assertThat(data).isEqualTo(events.get(0));
    }

    @Test
    public void testReconcile() throws IOException {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> stmts = asList(statement("uri:a", "uri:b", "uri:c"), statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.reconcile(() -> meta, "Q123", 123, eventTime, stmts);
        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw.toString()).isEqualTo(Resources.toString(TEST_RECONCILE_JSON_RESOURCE, UTF_8).replace("\n", ""));

        Object data = MapperUtils.getObjectMapper().readValue(TEST_RECONCILE_JSON_RESOURCE, MutationEventData.class);
        assertThat(data).isEqualTo(events.get(0));
    }

    @Test
    public void testDiff() throws IOException {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<Statement> deleted = singletonList(statement("uri:del", "uri:del", "uri:del"));
        List<Statement> unlinkedData = singletonList(statement("uri:unlinked", "uri:unlinked", "uri:unlinked"));
        List<MutationEventData> events = eventGenerator.diffEvent(() -> meta, "Q123", 123, eventTime, added, deleted, linkedData, unlinkedData);

        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw.toString()).isEqualTo(Resources.toString(TEST_DIFF_JSON_RESOURCE, UTF_8).replace("\n", ""));

        Object data = MapperUtils.getObjectMapper().readValue(TEST_DIFF_JSON_RESOURCE, MutationEventData.class);
        assertThat(data).isEqualTo(events.get(0));
    }

    @Test
    public void testDelete() throws IOException {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<MutationEventData> events = eventGenerator.deleteEvent(() -> meta, "Q123", 123, eventTime);

        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw.toString()).isEqualTo(Resources.toString(TEST_DELETE_JSON_RESOURCE, UTF_8).replace("\n", ""));

        Object data = MapperUtils.getObjectMapper().readValue(TEST_DELETE_JSON_RESOURCE, MutationEventData.class);
        assertThat(data).isEqualTo(events.get(0));
    }

    private MutationEventDataGenerator buildEventGenerator(int softMaxSize) {
        return new MutationEventDataGenerator(chunkSer, RDFFormat.TURTLE.getDefaultMIMEType(), softMaxSize);
    }
}
