package org.wikidata.query.rdf.updater.consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statement;
import static org.wikidata.query.rdf.test.StatementHelper.uri;

import java.time.Instant;
import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.RDFWriterRegistry;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataGenerator;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;
import org.wikidata.query.rdf.updater.RDFChunkSerializer;


public class UpdatePatchAccumulatorUnitTest {
    @Test
    public void test() {
        RDFChunkSerializer serDeser = new RDFChunkSerializer(RDFWriterRegistry.getInstance());
        MutationEventDataGenerator generator = new MutationEventDataGenerator(serDeser, RDFFormat.TURTLE.getDefaultMIMEType(), 10);
        List<MutationEventData> data = generator.diffEvent(() -> new EventsMeta(Instant.EPOCH, "", "", "", ""), "", 1, Instant.EPOCH,
                singletonList(stmt("uri:added")),
                singletonList(stmt("uri:deleted")),
                singletonList(stmt("uri:linked")),
                singletonList(stmt("uri:unlinked")));

        PatchAccumulator accum = new PatchAccumulator(new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance())));
        data.forEach(accum::accumulate);
        assertThat(accum.getTotalAccumulated()).isEqualTo(4);
        assertThat(accum.size()).isEqualTo(4);
        assertThat(accum.getAllAdded()).containsOnly(stmt("uri:added"));
        assertThat(accum.getAllRemoved()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getAllLinkedSharedElts()).containsOnly(stmt("uri:linked"));
        assertThat(accum.getAllUnlinkedSharedElts()).containsOnly(stmt("uri:unlinked"));

        accum.accumulate(
                singletonList(stmt("uri:A1")),
                emptyList(),
                singletonList(stmt("uri:L1")),
                emptyList()
        );
        accum.storeEntityIdsToDelete("entity123");

        assertThat(accum.getTotalAccumulated()).isEqualTo(6);
        assertThat(accum.size()).isEqualTo(6);
        assertThat(accum.getAllAdded()).containsOnly(stmt("uri:added"), stmt("uri:A1"));
        assertThat(accum.getAllRemoved()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getAllLinkedSharedElts()).containsOnly(stmt("uri:linked"), stmt("uri:L1"));
        assertThat(accum.getAllUnlinkedSharedElts()).containsOnly(stmt("uri:unlinked"));
        assertThat(accum.getAllEntitiesToDelete()).containsOnly("entity123");

        accum.accumulate(
                singletonList(stmt("uri:A2")),
                singletonList(stmt("uri:A1")),
                asList(stmt("uri:L2"), stmt("uri:shared")),
                singletonList(stmt("uri:L1"))
        );

        assertThat(accum.getTotalAccumulated()).isEqualTo(11);
        assertThat(accum.size()).isEqualTo(7);
        assertThat(accum.getAllAdded()).containsOnly(stmt("uri:added"), stmt("uri:A2"));
        assertThat(accum.getAllRemoved()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getAllLinkedSharedElts()).containsOnly(stmt("uri:linked"), stmt("uri:L2"), stmt("uri:shared"));
        assertThat(accum.getAllUnlinkedSharedElts()).containsOnly(stmt("uri:unlinked"));

        accum.accumulate(
                singletonList(stmt("uri:A1")),
                singletonList(stmt("uri:A2")),
                singletonList(stmt("uri:L1")),
                singletonList(stmt("uri:L2"))
        );

        assertThat(accum.getTotalAccumulated()).isEqualTo(15);
        assertThat(accum.size()).isEqualTo(7);
        assertThat(accum.getAllAdded()).containsOnly(stmt("uri:added"), stmt("uri:A1"));
        assertThat(accum.getAllRemoved()).containsOnly(stmt("uri:deleted"));
        assertThat(accum.getAllLinkedSharedElts()).containsOnly(stmt("uri:linked"), stmt("uri:L1"), stmt("uri:shared"));
        assertThat(accum.getAllUnlinkedSharedElts()).containsOnly(stmt("uri:unlinked"));
    }

    private Statement stmt(String s) {
        return statement(s, s, uri(s));
    }
}
