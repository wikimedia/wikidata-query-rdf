package org.wikidata.query.rdf.updater;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikimedia.eventutilities.core.event.EventSchemaLoader;
import org.wikimedia.eventutilities.core.event.EventSchemaValidator;
import org.wikimedia.eventutilities.core.json.JsonLoadingException;
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader;
import org.wikimedia.eventutilities.core.util.ResourceLoader;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.google.common.io.Resources;

@RunWith(Parameterized.class)
public class MutationEventDataJsonSerializationUnitTest {
    private final URL testImportJsonResource;
    private final URL testDiffJsonResource;
    private final URL testDeleteJsonResource;
    private final URL testReconcileJsonResource;

    private final RDFChunkSerializer chunkSer;
    private final MutationEventDataGenerator eventGenerator;
    private final MutationEventDataJsonDeserializer deserializer = new MutationEventDataJsonDeserializer();
    private final EventSchemaLoader eventSchemaLoader;
    private final EventSchemaValidator validator;
    private final boolean schemaValidation;

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[] {"v1"}, new Object[]{"v2"});
    }

    public MutationEventDataJsonSerializationUnitTest(String version) throws MalformedURLException {
        eventSchemaLoader = EventSchemaLoader
            .builder()
            .setJsonSchemaLoader(JsonSchemaLoader.build(ResourceLoader
                .builder()
                .addLoader("file", this::loadResource)
                .setBaseUrls(Collections.singletonList(new URL("file:/jsonschema_a9a757ca")))
                .build()))
            .build();
        validator = new EventSchemaValidator(eventSchemaLoader);
        chunkSer = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
        final MutationEventDataFactory factory;
        switch (version) {
            case "v1":
                factory = MutationEventDataFactory.v1();
                schemaValidation = false;
                break;
            case "v2":
                factory = MutationEventDataFactory.v2();
                schemaValidation = true;
                break;
            default:
                throw new IllegalArgumentException("Unknown version " + version);
        }
        eventGenerator = new MutationEventDataGenerator(
                chunkSer,
                RDFFormat.TURTLE.getDefaultMIMEType(),
                Integer.MAX_VALUE,
                factory);

        testImportJsonResource = MutationEventDataJsonSerializationUnitTest.class
                .getResource("MutationEventDataJsonSerializationUnitTest-testImport-" + version + ".json");
        testDiffJsonResource = MutationEventDataJsonSerializationUnitTest.class
                .getResource("MutationEventDataJsonSerializationUnitTest-testDiff-" + version + ".json");
        testDeleteJsonResource = MutationEventDataJsonSerializationUnitTest.class
                .getResource("MutationEventDataJsonSerializationUnitTest-testDelete-" + version + ".json");
        testReconcileJsonResource = MutationEventDataJsonSerializationUnitTest.class
                .getResource("MutationEventDataJsonSerializationUnitTest-testReconcile-" + version + ".json");
    }

    private byte[] loadResource(URI uri) {
        try {
            return Resources.toByteArray(MutationEventDataGeneratorUnitTest.class.getResource(uri.getPath()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MutationEventData deser(URL testResource) throws IOException {
        byte[] byteArray = Resources.toByteArray(testResource);
        if (schemaValidation) {
            try {
                ProcessingReport report = validator.validate(new String(byteArray, UTF_8));
                assertThat(report.isSuccess())
                        .withFailMessage(report.toString())
                        .isTrue();
            } catch (JsonLoadingException | ProcessingException e) {
                fail("Cannot validate event data", e);
            }
        }
        return deserializer.deserialize(byteArray);
    }

    @Test
    public void testImport() throws IOException {
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.fullImportEvent(() -> meta, "Q123", 123, eventTime, added, linkedData);
        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw).hasToString(Resources.toString(testImportJsonResource, UTF_8).replace("\n", ""));

        assertThat(deser(testImportJsonResource)).isEqualTo(events.get(0));
    }

    @Test
    public void testReconcile() throws IOException {
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> stmts = asList(statement("uri:a", "uri:b", "uri:c"), statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.reconcile(() -> meta, "Q123", 123, eventTime, stmts);
        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw).hasToString(Resources.toString(testReconcileJsonResource, UTF_8).replace("\n", ""));

        assertThat(deser(testReconcileJsonResource)).isEqualTo(events.get(0));
    }

    @Test
    public void testDiff() throws IOException {
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<Statement> deleted = singletonList(statement("uri:del", "uri:del", "uri:del"));
        List<Statement> unlinkedData = singletonList(statement("uri:unlinked", "uri:unlinked", "uri:unlinked"));
        List<MutationEventData> events = eventGenerator.diffEvent(() -> meta, "Q123", 123, eventTime, added, deleted, linkedData, unlinkedData);

        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw).hasToString(Resources.toString(testDiffJsonResource, UTF_8).replace("\n", ""));

        assertThat(deser(testDiffJsonResource)).isEqualTo(events.get(0));
    }

    @Test
    public void testDelete() throws IOException {
        EventsMeta meta = EventMetaUtil.makeEventMeta(new UUID(2, 3), new UUID(3, 4));
        Instant eventTime = Instant.EPOCH;
        List<MutationEventData> events = eventGenerator.deleteEvent(() -> meta, "Q123", 123, eventTime);

        StringWriter sw = new StringWriter();
        MapperUtils.getObjectMapper().writer().writeValue(sw, events.get(0));
        assertThat(sw).hasToString(Resources.toString(testDeleteJsonResource, UTF_8).replace("\n", ""));

        assertThat(deser(testDeleteJsonResource)).isEqualTo(events.get(0));
    }
}
