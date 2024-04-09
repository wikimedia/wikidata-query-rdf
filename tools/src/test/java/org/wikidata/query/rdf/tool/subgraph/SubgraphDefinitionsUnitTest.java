package org.wikidata.query.rdf.tool.subgraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public class SubgraphDefinitionsUnitTest {
    private final ValueFactory valueFactory = new ValueFactoryImpl();
    private final SubgraphDefinition name1 = buildDef("name1", valueFactory.createURI("uri:name1"));
    private final SubgraphDefinition name2 = buildDef("name2", valueFactory.createURI("uri:name2"));
    private final SubgraphDefinition noUri = buildDef("name3", null);
    private final SubgraphDefinition uriDup = buildDef("name4", valueFactory.createURI("uri:name1"));
    private final SubgraphDefinition noUri2 = buildDef("name5", null);

    private SubgraphDefinition buildDef(String name1, URI valueFactory) {
        return new SubgraphDefinition(name1, valueFactory, "something", null, SubgraphRule.Outcome.pass, false);
    }

    @Test
    public void testFindByName() {
        SubgraphDefinitions subgraphs = new SubgraphDefinitions(Arrays.asList(name1, name2));
        assertThat(subgraphs.getDefinitionByName("name1")).isEqualTo(name1);
        assertThat(subgraphs.getDefinitionByName("name4")).isNull();
    }

    @Test
    public void testFindByUri() {
        SubgraphDefinitions subgraphs = new SubgraphDefinitions(Arrays.asList(name1, name2, noUri, noUri2));
        assertThat(subgraphs.getDefinitionByUri(valueFactory.createURI("uri:name1"))).isEqualTo(name1);
        assertThat(subgraphs.getDefinitionByUri(valueFactory.createURI("uri:name4"))).isNull();
    }

    @Test
    public void testFailOnNameDups() {
        List<SubgraphDefinition> subgraphs = Arrays.asList(name1, name1);
        assertThatThrownBy(() -> new SubgraphDefinitions(subgraphs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate names in subgraph definitions: [name1]");
    }

    @Test
    public void testFailOnUriDups() {
        List<SubgraphDefinition> subgraphs = Arrays.asList(name1, name2, noUri, noUri2, uriDup);
        assertThatThrownBy(() -> new SubgraphDefinitions(subgraphs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate subgraph uris in subgraph definitions: [uri:name1]");
    }
}
