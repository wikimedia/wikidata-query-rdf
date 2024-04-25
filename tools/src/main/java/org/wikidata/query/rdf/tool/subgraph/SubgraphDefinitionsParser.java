package org.wikidata.query.rdf.tool.subgraph;

import java.io.IOException;
import java.io.InputStream;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class SubgraphDefinitionsParser {
    private final ObjectMapper objectMapper;
    private final ValueFactory valueFactory;

    public static SubgraphDefinitions parseYaml(InputStream is) throws IOException {
        return yamlParser().parse(is);
    }
    public static SubgraphDefinitionsParser yamlParser() {
        return new SubgraphDefinitionsParser(new YAMLFactory(), new ValueFactoryImpl());
    }

    public SubgraphDefinitionsParser(JsonFactory factory, ValueFactory valueFactory) {
        this.objectMapper = new ObjectMapper(factory);
        this.valueFactory = valueFactory;
    }

    public SubgraphDefinitions parse(InputStream is) throws IOException {
        return objectMapper.reader()
                .withAttribute(SubgraphDefinitions.VALUE_FACTORY_ATTRIBUTE, valueFactory)
                .forType(SubgraphDefinitions.class)
                .readValue(is);
    }
}
