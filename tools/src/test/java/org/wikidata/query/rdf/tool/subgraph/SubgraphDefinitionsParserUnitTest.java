package org.wikidata.query.rdf.tool.subgraph;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.ImmutableMap;

public class SubgraphDefinitionsParserUnitTest {
    private final ValueFactory valueFactory = new ValueFactoryImpl();

    @Test
    public void testParse() throws IOException {
        SubgraphDefinitionsParser parser = SubgraphDefinitionsParser.yamlParser(
                ImmutableMap.of(
                        "wdt", "http://www.wikidata.org/prop/direct/",
                        "wd", "http://www.wikidata.org/entity/",
                        "rdfs", "http://www.w3.org/2000/01/rdf-schema#",
                        "wikibase", "http://wikiba.se/ontology#",
                        "wdsubgraph", "https://query.wikidata.org/subgraph/"
                )
        );
        SubgraphDefinitions definitions = parser.parse(this.getClass().getResourceAsStream("/subgraph-definitions.yaml"));
        assertThat(definitions.getSubgraphs()).hasSize(3);
        SubgraphDefinition main = definitions.getDefinitionByName("main");
        assertThat(main).isNotNull();
        assertThat(main.getSubgraphUri()).isEqualTo(valueFactory.createURI("https://query.wikidata.org/subgraph/main"));
        assertThat(main.getStream()).isEqualTo("rdf-streaming-updater.mutations-main");
        assertThat(main.getRules())
                .containsExactly(new SubgraphRule(SubgraphRule.Outcome.block,
                        new SubgraphRule.TriplePattern(
                                valueFactory.createBNode("entity"),
                                valueFactory.createURI("http://www.wikidata.org/prop/direct/P31"),
                                valueFactory.createURI("http://www.wikidata.org/entity/Q13442814"))));
        assertThat(main.isStubsSource()).isTrue();
    }

    @Test
    public void testMissingPrefix() {
        SubgraphDefinitionsParser parser = SubgraphDefinitionsParser.yamlParser(emptyMap());
        InputStream is = this.getClass().getResourceAsStream("/subgraph-definitions.yaml");
        assertThatThrownBy(() -> parser.parse(is))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unknown prefix: ");
    }

    @Test
    public void testInvalidRule() {
        SubgraphDefinitionsParser parser = SubgraphDefinitionsParser.yamlParser(emptyMap());
        InputStream is = this.getClass().getResourceAsStream("/subgraph-definitions-invalid-rules.yaml");
        assertThatThrownBy(() -> parser.parse(is))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid rule definition: garbage");
    }
}
