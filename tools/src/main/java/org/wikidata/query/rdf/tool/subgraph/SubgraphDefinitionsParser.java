package org.wikidata.query.rdf.tool.subgraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class SubgraphDefinitionsParser {
    private final ObjectMapper objectMapper;
    private final Map<String, String> prefixes;
    private final ValueFactory valueFactory;

    public static SubgraphDefinitionsParser yamlParser(Map<String, String> prefixes) {
        return new SubgraphDefinitionsParser(new YAMLFactory(), prefixes, new ValueFactoryImpl());
    }

    public SubgraphDefinitionsParser(JsonFactory factory, Map<String, String> prefixes, ValueFactory valueFactory) {
        this.objectMapper = new ObjectMapper(factory);
        this.prefixes = prefixes;
        this.valueFactory = valueFactory;
    }

    public SubgraphDefinitions parse(InputStream is) throws IOException {
        return objectMapper.reader()
                .withAttribute(SubgraphDefinitions.PREFIXES_ATTRIBUTE, prefixes)
                .withAttribute(SubgraphDefinitions.VALUE_FACTORY_ATTRIBUTE, valueFactory)
                .forType(SubgraphDefinitions.class)
                .readValue(is);
    }
}
