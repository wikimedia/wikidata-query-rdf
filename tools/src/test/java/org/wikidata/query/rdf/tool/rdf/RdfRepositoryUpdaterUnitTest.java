package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

@RunWith(MockitoJUnitRunner.class)
public class RdfRepositoryUpdaterUnitTest {
    @Mock
    RdfClient client;

    @Test
    public void testInsertOnly() {
        Patch patch = new Patch(statements("uri:a", "uri:b"), statements(), statements(), statements());
        when(client.update(anyString())).thenReturn(2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update("INSERT DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n");
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(0);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(0);
    }

    @Test
    public void testDeleteOnly() {
        Patch patch = new Patch(statements(), statements(), statements("uri:a", "uri:b"), statements());
        when(client.update(anyString())).thenReturn(2);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
        RDFPatchResult result = rdfRepositoryUpdater.applyPatch(patch);
        verify(client).update("DELETE DATA {\n<uri:a> <uri:a> <uri:a> .\n<uri:b> <uri:b> <uri:b> .\n};\n");
        assertThat(result.getActualMutations()).isEqualTo(2);
        assertThat(result.getExpectedMutations()).isEqualTo(2);
        assertThat(result.getActualSharedElementsMutations()).isEqualTo(0);
        assertThat(result.getPossibleSharedElementMutations()).isEqualTo(0);
    }

    @Test
    public void testInsertDeleteOnly() {
        Patch patch = new Patch(statements("uri:a", "uri:b"), statements(), statements("uri:c", "uri:d"), statements());
        when(client.update(anyString())).thenReturn(3);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
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
        Patch patch = new Patch(statements(), statements(), statements(), statements());
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
        rdfRepositoryUpdater.applyPatch(patch);
    }

    @Test
    public void testInsertSharedElts() {
        Patch patch = new Patch(statements("uri:a", "uri:b"), statements("uri:s1", "uri:s2"), statements(), statements());
        when(client.update(anyString())).thenReturn(2, 1);
        RdfRepositoryUpdater rdfRepositoryUpdater = new RdfRepositoryUpdater(client);
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
}
