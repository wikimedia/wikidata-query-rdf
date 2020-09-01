package org.wikidata.query.rdf.updater;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.comparator;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;

public class RDFChunkSerializerUnitTest {
    private final RDFChunkDeserializer deser = new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance()));
    private final RDFChunkSerializer ser = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
    private final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void testRoundTrip() {
        List<Statement> statementList = Arrays.asList(
                statement("uri:s1", "uri:size", vf.createLiteral(1)),
                statement("uri:s1", "uri:byte", vf.createLiteral((byte) 1)),
                statement("uri:s1", "uri:short", vf.createLiteral((short)2)),
                statement("uri:s1", "uri:long", vf.createLiteral(3L)),
                statement("uri:s1", "uri:float", vf.createLiteral(1.0F)),
                statement("uri:s1", "uri:double", vf.createLiteral(2D)),
                statement("uri:s1", "uri:date", vf.createLiteral(Date.from(Instant.EPOCH))),
                statement("uri:s1", "uri:bnode", vf.createBNode("1")),
                statement("uri:s1", "uri:uri", "uri:object"),
                statement("uri:s1", "uri:string", vf.createLiteral("string")),
                statement("uri:s1", "uri:localizedString", vf.createLiteral("string", "fr"))
        );

        List<RDFDataChunk> data = ser.serializeAsChunks(statementList, "text/turtle", Integer.MAX_VALUE);
        assertThat(data.size()).isOne();
        RDFDataChunk chunk = data.get(0);
        int l = chunk.getData().length();
        List<Statement> deserializedStmts = deser.deser(chunk, "unused");
        assertThat(deserializedStmts)
                .usingComparatorForType(comparator(), Statement.class)
                .containsExactlyElementsOf(statementList);

        data = ser.serializeAsChunks(statementList, "text/turtle", l / 4);
        assertThat(data.size()).isGreaterThan(1);
        deserializedStmts = data.stream().flatMap(d -> deser.deser(d, "unused").stream()).collect(toList());
        assertThat(deserializedStmts)
                .usingComparatorForType(comparator(), Statement.class)
                .containsExactlyElementsOf(statementList);
    }
}
