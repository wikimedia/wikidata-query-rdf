package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

@RunWith(MockitoJUnitRunner.class)
public class RdfRepositoryUpdaterUnitTest {
    @Mock
    RdfClient client;

    private UrisScheme urisScheme = UrisSchemeFactory.forHost("acme.test");
    List<String> entityIdsToDelete = new ArrayList<>();

    @Test
    public void testInsertOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements(), statements(), statements(), entityIdsToDelete);
        when(client.update(anyString())).thenReturn(2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update("INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n");
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(0);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(0);
    }

    @Test
    public void testDeleteOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements("uri:a", "uri:b"), statements(), entityIdsToDelete);
        when(client.update(anyString())).thenReturn(2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update("DELETE DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n");
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(0);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(0);
    }

    @Test
    public void testInsertDeleteOnly() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements(), statements("uri:c", "uri:d"), statements(), entityIdsToDelete);
        when(client.update(anyString())).thenReturn(3);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update(
                "DELETE DATA {\n" +
                    "<uri:c> <uri:c> <uri:c> .\n" +
                    "<uri:d> <uri:d> <uri:d> .\n" +
                    "};\n" +
                "INSERT DATA {\n" +
                    "<uri:a> <uri:a> <uri:a> .\n" +
                    "<uri:b> <uri:b> <uri:b> .\n" +
                    "};\n");
        assertThat(result.getActualMutations()).isEqualTo(3);
        assertThat(result.getExpectedMutations()).isEqualTo(4);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(0);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsOnEmptyPatch() {
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(), entityIdsToDelete);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        rdfRepositoryUpdater.applyPatch(patch);
    }

    @Test
    public void testInsertSharedElts() {
        ConsumerPatch patch = new ConsumerPatch(statements("uri:a", "uri:b"), statements("uri:s1", "uri:s2"), statements(), statements(), entityIdsToDelete);
        when(client.update(anyString())).thenReturn(2, 1);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).update(captor.capture());

        assertThat(captor.getAllValues()).containsExactly(
                "INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n",
                "INSERT DATA {\n<uri:s1> <uri:s1> <uri:s1> .\n<uri:s2> <uri:s2> <uri:s2> .\n};\n"
        );
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(1);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(2);
    }

    @Test
    public void testDeleteEntities() {
        entityIdsToDelete.add("entity123");
        ConsumerPatch patch = new ConsumerPatch(statements(), statements(), statements(), statements(), entityIdsToDelete);
        when(client.update(anyString())).thenReturn(2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client, urisScheme);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update(deleteQueryString);
        assertThat(result.getDeleteMutations()).isEqualTo(2);
    }

    private final String deleteQueryString = "# clear all statements (except shared) about an entity\n" +
            "DELETE {\n" +
            "  ?entityStatements ?statementPredicate ?statementObject .\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "     <http://acme.test/entity/entity123>\n" +
            "  }\n" +
            "  ?entity ?entityToStatementPredicate ?entityStatements .\n" +
            "  FILTER( STRSTARTS(STR(?entityStatements), \"http://acme.test/entity/statement/\") ) .\n" +
            "  ?entityStatements ?statementPredicate ?statementObject .\n" +
            "};\n" +
            "\n" +
            "DELETE {\n" +
            "    ?siteLink ?sitelinkPredicate ?sitelinkObject\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "     <http://acme.test/entity/entity123>\n" +
            "  }\n" +
            "  ?siteLink <http://schema.org/about> ?entity .\n" +
            "  ?siteLink ?sitelinkPredicate ?sitelinkObject .\n" +
            "};\n" +
            "\n" +
            "DELETE {\n" +
            "  ?entity ?entityPredicate ?entityObject .\n" +
            "}\n" +
            "WHERE {\n" +
            "  VALUES ?entity {\n" +
            "      <http://acme.test/entity/entity123>\n" +
            "  }\n" +
            "  ?entity ?entityPredicate ?entityObject .\n" +
            "};";
}
