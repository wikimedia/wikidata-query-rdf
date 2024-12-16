package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statement;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetricsResponseHandler;

@RunWith(MockitoJUnitRunner.class)
public class RdfRepositoryUpdaterUnitTest {
    @Mock
    RdfClient client;

    private UrisScheme urisScheme = UrisSchemeFactory.forWikidataHost("acme.test");
    List<String> entityIdsToDelete = new ArrayList<>();
    Map<String, Collection<Statement>> entitiesToReconcile = new HashMap<>();

    private final String avgEventTimeUpdate = "\nDELETE {\n  <" + UrisSchemeFactory.getURISystem().root() + "> <http://schema.org/dateModified> ?o .\n}\n" +
                    "WHERE {\n  <" + UrisSchemeFactory.getURISystem().root() + "> <http://schema.org/dateModified> ?o .\n};\n" +
                    "INSERT DATA {\n  <" + UrisSchemeFactory.getURISystem().root() + "> <http://schema.org/dateModified> " +
                    "\"1970-01-01T00:10:00Z\"^^xsd:dateTime .\n};\n";
    Instant avgEventTime = Instant.EPOCH.plus(10, ChronoUnit.MINUTES);
    private final String deleteQueryString = IOUtils.toString(this.getClass()
            .getResourceAsStream("/org/wikidata/query/rdf/tool/rdf/RdfRepositoryUpdater.deleteEntity.expected"));

    public RdfRepositoryUpdaterUnitTest() throws IOException {
    }

    @Test
    public void testInsertOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements(), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        when(client.update(anyString())).thenReturn(2, 1);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());

        assertThat(captor.getAllValues()).containsExactly(
                "INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n",
                avgEventTimeUpdate);
        verify(client).update("INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n");
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(1);
    }

    @Test
    public void testDeleteOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements("uri:a", "uri:b"), statements(),
                entityIdsToDelete, entitiesToReconcile);
        when(client.update(anyString())).thenReturn(2, 1);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());
        assertThat(captor.getAllValues()).containsExactly(
                "DELETE DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n",
                avgEventTimeUpdate);
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(1);
    }

    @Test
    public void testInsertDeleteOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements(), statements("uri:c", "uri:d"), statements(),
                entityIdsToDelete, entitiesToReconcile);
        when(client.update(anyString())).thenReturn(3, 1);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());
        assertThat(captor.getAllValues()).containsExactly(
                "DELETE DATA {\n" +
                        "<uri:c> <uri:c> <uri:c> .\n" +
                        "<uri:d> <uri:d> <uri:d> .\n" +
                        "};\n" +
                        "INSERT DATA {\n" +
                        "<uri:a> <uri:a> <uri:a> .\n" +
                        "<uri:b> <uri:b> <uri:b> .\n" +
                        "};\n",
                avgEventTimeUpdate);
        assertThat(result.getActualMutations()).isEqualTo(3);
        assertThat(result.getExpectedMutations()).isEqualTo(4);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(1);
    }

    @Test
    public void testInsertOnlySharedElts() {
        ConsumerPatch patch = new ConsumerPatch(statements(), statements("uri:s1", "uri:s2"), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        final ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).update(queryCaptor.capture());
        assertThat(queryCaptor.getValue()).contains("uri:s1", "uri:s2");
    }

    @Test
    public void testInsertSharedElts() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements("uri:s1", "uri:s2"),
                statements(), statements("uri:s3", "uri:s4"), entityIdsToDelete, entitiesToReconcile);
        when(client.update(anyString())).thenReturn(2, 2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());

        assertThat(captor.getAllValues()).containsExactly(
                "INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n",
                "INSERT DATA {\n<uri:s1> <uri:s1> <uri:s1> .\n<uri:s2> <uri:s2> <uri:s2> .\n};\n" +
                        avgEventTimeUpdate
        );
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(2);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(3);
    }

    @Test
    public void testDeleteEntities() {
        entityIdsToDelete.add("entity123");
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(),
                entityIdsToDelete, entitiesToReconcile);
        when(client.update(anyString())).thenReturn(1, 2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());

        assertThat(captor.getAllValues()).containsExactly(
                avgEventTimeUpdate,
                deleteQueryString
        );
        assertThat(result.getDeleteMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(1);
    }

    @Test
    public void testReconcile() {
        entitiesToReconcile.put("Q123", Arrays.asList(
                statement("http://acme.test/entity/Q123", "http://actme.test/prop/direct/P123", "uri:something"),
                statement("http://acme.test/entity/Q123", "http://actme.test/prop/P123", "http://acme.test/entity/statement/Q123-S-123"),
                statement("http://acme.test/entity/statement/Q123-S-123", "http://actme.test/property/P123", "http://acme.test/property/statement/Q123"),
                statement("https://oc.wikipedia.org/Affachade", "http://actme.test/property/P123", "http://acme.test/entity/Q123")));
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(), entityIdsToDelete, entitiesToReconcile);

        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        UpdateMetricsResponseHandler handler = new UpdateMetricsResponseHandler(false, false, true);
        CollectedUpdateMetrics metrics = new CollectedUpdateMetrics();
        metrics.setMutationCount(10);
        when(client.update(anyString(), any())).thenReturn(metrics);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch, avgEventTime);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).update(captor.capture(), any());
        assertThat(result.getReconciliationMutations()).isEqualTo(10);

        assertThat(captor.getValue()).isEqualTo(reconciliationQueryString);
    }
    private final String reconciliationQueryString = "# Clear out of date site links\n" +
            "DELETE {\n" +
            "  ?s ?p ?o .\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "     <http://acme.test/entity/Q123>\n" +
            "  }\n" +
            "  ?s <http://schema.org/about> ?entity .\n" +
            "  ?s ?p ?o .\n" +
            "  # This construct is constantly reused throughout the updates.  Its job is to not delete statements\n" +
            "  # that are still in use.\n" +
            "  MINUS {\n" +
            "    VALUES ( ?s ?p ?o ) {\n" +
            "      ( <https://oc.wikipedia.org/Affachade> <http://actme.test/property/P123> <http://acme.test/entity/Q123> )\n" +
            "    }\n" +
            "  }\n" +
            "};\n" +
            "# Clear out of date statements about statements\n" +
            "DELETE {\n" +
            "  ?s ?p ?o .\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "     <http://acme.test/entity/Q123>\n" +
            "  }\n" +
            "  ?entity ?statementPred ?s .\n" +
            "  FILTER( STRSTARTS(STR(?s), \"http://acme.test/entity/statement/\") ) .\n" +
            "  ?s ?p ?o .\n" +
            "  MINUS {\n" +
            "    VALUES ( ?s ?p ?o ) {\n" +
            "      ( <http://acme.test/entity/statement/Q123-S-123> <http://actme.test/property/P123> <http://acme.test/property/statement/Q123> )\n" +
            "    }\n" +
            "  }\n" +
            "};\n" +
            "# Clear out of date statements about the entity\n" +
            "DELETE {\n" +
            "  ?entity ?p ?o .\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "       <http://acme.test/entity/Q123>\n" +
            "  }\n" +
            "  ?entity ?p ?o .\n" +
            "  MINUS {\n" +
            "    VALUES ( ?entity ?p ?o ) {\n" +
            "      ( <http://acme.test/entity/Q123> <http://actme.test/prop/direct/P123> <uri:something> )\n" +
            "( <http://acme.test/entity/Q123> <http://actme.test/prop/P123> <http://acme.test/entity/statement/Q123-S-123> )\n" +
            "    }\n" +
            "  }\n" +
            "};\n" +
            "# Insert new data\n" +
            "INSERT DATA {\n" +
            "  <http://acme.test/entity/Q123> <http://actme.test/prop/direct/P123> <uri:something> .\n" +
            "<http://acme.test/entity/Q123> <http://actme.test/prop/P123> <http://acme.test/entity/statement/Q123-S-123> .\n" +
            "<http://acme.test/entity/statement/Q123-S-123> <http://actme.test/property/P123> <http://acme.test/property/statement/Q123> .\n" +
            "<https://oc.wikipedia.org/Affachade> <http://actme.test/property/P123> <http://acme.test/entity/Q123> .\n" +
            "};";
}
