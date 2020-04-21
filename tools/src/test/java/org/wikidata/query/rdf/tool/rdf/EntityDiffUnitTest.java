package org.wikidata.query.rdf.tool.rdf;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;

@RunWith(MockitoJUnitRunner.class)
public class EntityDiffUnitTest {
    @Mock
    UrisScheme urisScheme;

    @Test
    public void testDiffNoSharedElt() {
        List<Statement> rev1 = asList(
                statement("uri:s1", "uri:p1", "uri:v1"),
                statement("uri:s2", "uri:p2", "uri:v2")
        );
        List<Statement> rev2 = asList(
                statement("uri:s1", "uri:p1", "uri:v1"),
                statement("uri:s3", "uri:p3", "uri:v3")
        );
        EntityDiff differ = new EntityDiff(stmt -> false);
        EntityDiff.Patch patch = differ.diff(rev1, rev2);
        assertThat(patch.getAdded()).containsExactly(
                statement("uri:s3", "uri:p3", "uri:v3")
        );
        assertThat(patch.getLinkedSharedElements()).isEmpty();

        assertThat(patch.getRemoved()).containsExactly(
                statement("uri:s2", "uri:p2", "uri:v2")
        );
        assertThat(patch.getUnlinkedSharedElements()).isEmpty();
    }

    @Test
    public void testDiffWithSharedElt() {
        List<Statement> rev1 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:s2", "uri:p", "uri:sharedres2"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
        List<Statement> rev2 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:s3", "uri:p", "uri:sharedres3"),
                statement("uri:sharedres3", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres3", "uri:p", "uri:shared_data2")
        );
        EntityDiff differ = new EntityDiff(stmt -> stmt.getSubject().toString().contains(":sharedres"));
        EntityDiff.Patch patch = differ.diff(rev1, rev2);
        assertThat(patch.getAdded()).containsOnly(
                statement("uri:s3", "uri:p", "uri:sharedres3")
        );
        assertThat(patch.getLinkedSharedElements()).containsOnly(
                statement("uri:sharedres3", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres3", "uri:p", "uri:shared_data2")
        );

        assertThat(patch.getRemoved()).containsOnly(
                statement("uri:s2", "uri:p", "uri:sharedres2")
        );
        assertThat(patch.getUnlinkedSharedElements()).containsOnly(
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
    }

    @Test
    public void testBasicImport() {
        List<Statement> rev1 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:s2", "uri:p", "uri:sharedres2"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
        EntityDiff differ = new EntityDiff(stmt -> stmt.getSubject().toString().contains(":sharedres"));
        EntityDiff.Patch patch = differ.diff(emptyList(), rev1);
        assertThat(patch.getAdded()).containsOnly(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:s2", "uri:p", "uri:sharedres2")
        );
        assertThat(patch.getLinkedSharedElements()).containsOnly(
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
        assertThat(patch.getRemoved()).isEmpty();
        assertThat(patch.getUnlinkedSharedElements()).isEmpty();
    }

    @Test
    public void testFullDeletion() {
        List<Statement> rev1 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:s2", "uri:p", "uri:sharedres2"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
        EntityDiff differ = new EntityDiff(stmt -> stmt.getSubject().toString().contains(":sharedres"));
        EntityDiff.Patch patch = differ.diff(rev1, emptyList());
        assertThat(patch.getRemoved()).containsOnly(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s1", "uri:p", "uri:sharedres1"),
                statement("uri:s2", "uri:p", "uri:sharedres2")
        );
        assertThat(patch.getUnlinkedSharedElements()).containsOnly(
                statement("uri:sharedres1", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres1", "uri:p", "uri:shared_data2"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data1"),
                statement("uri:sharedres2", "uri:p", "uri:shared_data2")
        );
        assertThat(patch.getAdded()).isEmpty();
        assertThat(patch.getLinkedSharedElements()).isEmpty();
    }

    @Test
    public void testValuesAndRefs() {
        List<Statement> rev1 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s2", "uri:p", "uri:v2"),
                statement("v:sharedres1", "uri:p", "uri:shared_data1")
        );
        List<Statement> rev2 = asList(
                statement("uri:s1", "uri:p", "uri:v1"),
                statement("uri:s3", "uri:p", "uri:v3"),
                statement("r:sharedres1", "uri:p", "uri:shared_data1")
        );
        when(urisScheme.value()).thenReturn("v:");
        when(urisScheme.reference()).thenReturn("r:");
        EntityDiff differ = EntityDiff.withValuesAndRefsAsSharedElements(urisScheme);
        EntityDiff.Patch patch = differ.diff(rev1, rev2);
        assertThat(patch.getAdded()).containsOnly(statement("uri:s3", "uri:p", "uri:v3"));
        assertThat(patch.getLinkedSharedElements()).containsOnly(statement("r:sharedres1", "uri:p", "uri:shared_data1"));
        assertThat(patch.getRemoved()).containsOnly(statement("uri:s2", "uri:p", "uri:v2"));
        assertThat(patch.getUnlinkedSharedElements()).containsOnly(statement("v:sharedres1", "uri:p", "uri:shared_data1"));
    }
}
