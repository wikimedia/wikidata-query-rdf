package org.wikidata.query.rdf.updater;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.wikidata.query.rdf.tool.MapperUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MutationEventDataJsonDeserializer {
    private final ObjectMapper mapper = MapperUtils.getObjectMapper();

    public MutationEventData deserialize(byte[] bytes) throws IOException {
        try (JsonParser parser = mapper.createParser(bytes, 0, bytes.length)) {
            JsonNode node = parser.readValueAsTree();
            if (node == null) {
                throw new JsonMappingException(parser, "Cannot parse json data: " + new String(bytes, StandardCharsets.UTF_8));
            }
            switch (node.get("$schema").textValue()) {
                case MutationEventDataV1.SCHEMA:
                    return mapper.treeToValue(node, MutationEventDataV1.class);
                case MutationEventDataV2.SCHEMA:
                    return mapper.treeToValue(node, MutationEventDataV2.class);
                default:
                    throw new UnsupportedOperationException("Unsupported schema " + node.get("$schema").textValue());
            }
        }
    }
}
