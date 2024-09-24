package org.wikidata.query.rdf.updater;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.StringWriter;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;


public class MutationEventDataGeneratorUnitTest {
    private final RDFWriterFactory ttlWriterFactory;
    private final String mime = RDFFormat.TURTLE.getDefaultMIMEType();
    private final RDFChunkSerializer chunkSer;

    public MutationEventDataGeneratorUnitTest() {
        ttlWriterFactory = RDFWriterRegistry.getInstance().get(RDFWriterRegistry.getInstance().getFileFormatForMIMEType(RDFFormat.TURTLE.getDefaultMIMEType()));
        chunkSer = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
    }

    private void assertSerializable(Collection<MutationEventData> data) {
        assertThat(
                data.stream()
                        .map(SerializationUtils::serialize)
                        .map(SerializationUtils::deserialize)
                        .collect(Collectors.toList())
        ).containsExactlyElementsOf(data);
    }

    @Test
    public void testSimpleImport() {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta();
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.fullImportEvent(() -> meta, "Q123", 123L, eventTime, added, linkedData);

        assertThat(events).containsExactly(new DiffEventDataV2(meta, "Q123", 123L, eventTime, 0, 1, MutationEventDataV2.IMPORT_OPERATION,
                chunk(added),
                null,
                chunk(linkedData),
                null));
        assertSerializable(events);
    }

    @Test
    public void testSimpleReconciliation() {
        MutationEventDataGenerator eventDataGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta();
        Instant eventTime = Instant.EPOCH;
        List<Statement> stmts = asList(statement("uri:a", "uri:b", "uri:c"),
                statement("uri:x", "uri:y", "uri:z"));

        List<MutationEventData> events = eventDataGenerator.reconcile(() -> meta, "Q123", 123L, eventTime, stmts);
        assertThat(events).containsExactly(new DiffEventDataV2(meta, "Q123", 123L, eventTime, 0, 1, MutationEventDataV2.RECONCILE_OPERATION,
                chunk(stmts),
                null,
                null,
                null));
        assertSerializable(events);
    }

    @Test
    public void testImportChunked() {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(1);
        EventsMeta meta = EventMetaUtil.makeEventMeta();
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<MutationEventData> events = eventGenerator.fullImportEvent(() -> meta, "Q123", 123L, eventTime, added, linkedData);

        assertThat(events).containsExactly(
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 0, 2, MutationEventDataV2.IMPORT_OPERATION,
                        chunk(added), null, null, null),
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 1, 2, MutationEventDataV2.IMPORT_OPERATION,
                        null, null, chunk(linkedData), null)
        );
        assertSerializable(events);
    }

    @Test
    public void testSimpleDiff() {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(Integer.MAX_VALUE);
        EventsMeta meta = EventMetaUtil.makeEventMeta();
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<Statement> deleted = singletonList(statement("uri:del", "uri:del", "uri:del"));
        List<Statement> unlinkedData = singletonList(statement("uri:unlinked", "uri:unlinked", "uri:unlinked"));
        List<MutationEventData> events = eventGenerator.diffEvent(() -> meta, "Q123", 123L, eventTime, added, deleted, linkedData, unlinkedData);

        assertThat(events).containsExactly(
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 0, 1, MutationEventDataV2.DIFF_OPERATION,
                        chunk(added), chunk(deleted), chunk(linkedData), chunk(unlinkedData))
        );
        assertSerializable(events);
    }

    @Test
    public void testDiffChunked() {
        MutationEventDataGenerator eventGenerator = buildEventGenerator(1);
        EventsMeta meta = EventMetaUtil.makeEventMeta();
        Instant eventTime = Instant.EPOCH;
        List<Statement> added = singletonList(statement("uri:a", "uri:b", "uri:c"));
        List<Statement> linkedData = singletonList(statement("uri:x", "uri:y", "uri:z"));
        List<Statement> deleted = singletonList(statement("uri:del", "uri:del", "uri:del"));
        List<Statement> unlinkedData = singletonList(statement("uri:unlinked", "uri:unlinked", "uri:unlinked"));
        List<MutationEventData> events = eventGenerator.diffEvent(() -> meta, "Q123", 123L, eventTime, added, deleted, linkedData, unlinkedData);

        assertThat(events).containsExactly(
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 0, 4, MutationEventDataV2.DIFF_OPERATION,
                        chunk(added), null, null, null),
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 1, 4, MutationEventDataV2.DIFF_OPERATION,
                        null, chunk(deleted), null, null),
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 2, 4, MutationEventDataV2.DIFF_OPERATION,
                        null, null, chunk(linkedData), null),
                new DiffEventDataV2(meta, "Q123", 123L, eventTime, 3, 4, MutationEventDataV2.DIFF_OPERATION,
                        null, null, null, chunk(unlinkedData))
        );
        assertSerializable(events);
    }

    private RDFDataChunk chunk(List<Statement> added) {
        return new RDFDataChunk(getTurtleOutput(added), mime);
    }

    private MutationEventDataGenerator buildEventGenerator(int softMaxSize) {
        return new MutationEventDataGenerator(chunkSer, RDFFormat.TURTLE.getDefaultMIMEType(), softMaxSize, MutationEventDataFactory.v2());
    }

    private String getTurtleOutput(Collection<Statement> stmts) {
        StringWriter sw = new StringWriter();
        RDFWriter writer = ttlWriterFactory.getWriter(sw);
        try {
            writer.startRDF();
            for (Statement stmt : stmts) {
                writer.handleStatement(stmt);
            }
            writer.endRDF();
        } catch (RDFHandlerException e) {
            fail("Cannot generate Turtle data", e);
        }
        return sw.toString();
    }
}
