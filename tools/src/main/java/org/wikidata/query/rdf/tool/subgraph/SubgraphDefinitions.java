package org.wikidata.query.rdf.tool.subgraph;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;

import lombok.Value;
import lombok.val;

@Value
@JsonDeserialize(using = SubgraphDefinitions.SubgraphDefinitionsDeserializer.class)
public class SubgraphDefinitions {
    static final String PREFIXES_ATTRIBUTE = SubgraphDefinitions.class.getName() + "-prefixes";
    static final String VALUE_FACTORY_ATTRIBUTE = SubgraphDefinitions.class.getName() + "-value-factory";
    private static final Pattern PREFIX = Pattern.compile("^([a-zA-Z\\d]+):(.*)$");
    Map<String, String> prefixes;
    List<SubgraphDefinition> subgraphs;

    public SubgraphDefinitions(List<SubgraphDefinition> subgraphs) {
        this(Collections.emptyMap(), subgraphs);
    }

    public SubgraphDefinitions(Map<String, String> prefixes, List<SubgraphDefinition> subgraphs) {
        this.prefixes = prefixes;
        val nameDups = duplicates(subgraphs, SubgraphDefinition::getName);
        if (!nameDups.isEmpty()) {
            throw new IllegalArgumentException("Duplicate names in subgraph definitions: " + nameDups);
        }
        val uriDups = duplicates(subgraphs, SubgraphDefinition::getSubgraphUri);
        if (!uriDups.isEmpty()) {
            throw new IllegalArgumentException("Duplicate subgraph uris in subgraph definitions: " + uriDups);
        }
        this.subgraphs = subgraphs;
    }

    private static <E> List<E> duplicates(Collection<SubgraphDefinition> subgraphs, Function<SubgraphDefinition, E> field) {
        return subgraphs.stream()
                .filter(s -> field.apply(s) != null)
                .collect(groupingBy(field, Collectors.counting()))
                .entrySet().stream()
                .filter(en -> en.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(toList());
    }

    public @Nullable SubgraphDefinition getDefinitionByName(String name) {
        Objects.requireNonNull(name);
        return subgraphs.stream().filter(e -> name.equals(e.getName())).findFirst().orElse(null);
    }

    public @Nullable SubgraphDefinition getDefinitionByUri(URI uri) {
        Objects.requireNonNull(uri);
        return subgraphs.stream().filter(e -> uri.equals(e.getSubgraphUri())).findFirst().orElse(null);
    }

    static URI parseUri(String literal, ValueFactory valueFactory, Map<String, String> prefixes) {
        if (literal.startsWith("<") && literal.endsWith(">")) {
            return valueFactory.createURI(literal.substring(1, literal.length() - 1));
        } else {
            Matcher m = PREFIX.matcher(literal);
            if (!m.find()) {
                throw new IllegalArgumentException("Cannot parse URI: " + literal);
            }

            String prefix = m.group(1);
            String ns = prefixes.get(prefix);
            if (ns == null) {
                throw new IllegalArgumentException("Unknown prefix: " + prefix);
            }
            return valueFactory.createURI(ns, m.group(2));
        }
    }

    static class SubgraphDefinitionsDeserializer extends JsonDeserializer<SubgraphDefinitions> {
        @Override
        @SuppressWarnings({"CyclomaticComplexity"})
        public SubgraphDefinitions deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            Map<String, String> prefixes = emptyMap();
            // buffer the list of definitions as an JsonNode object so that it can be mapped later once prefixes are
            // collected.
            ArrayNode subgraphs = null;
            if (p.currentToken() != JsonToken.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT");
            }
            JsonToken token;
            while (true) {
                token = p.nextToken();
                if (token == JsonToken.END_OBJECT) {
                    break;
                }
                if (token != JsonToken.FIELD_NAME) {
                    throw new JsonParseException(p, "Expected a FIELD_NAME or END_OBJECT token got " + p.currentToken());
                }
                switch (p.currentName()) {
                    case "prefixes":
                        if (p.nextToken() != JsonToken.START_OBJECT) {
                            throw new JsonParseException(p, "[prefixes] expected a START_OBJECT token got " + p.currentToken());
                        }
                        prefixes = p.readValueAs(new TypeReference<Map<String, String>>(){});
                        break;
                    case "subgraphs":
                        p.nextToken();
                        JsonNode subgraphsNode = p.readValueAsTree();
                        if (!subgraphsNode.isArray()) {
                            throw new JsonParseException(p, "subgraphs must be an array, got " + subgraphsNode.getNodeType());
                        }
                        subgraphs = (ArrayNode) subgraphsNode;
                        break;
                    default: throw new JsonParseException(p, "Unsupported field " + p.currentName());
                }
            }

            if (subgraphs == null) {
                throw new IllegalArgumentException("Expected at least one subgraph definition");
            }

            // ugly hack to parse the list of subgraph definitions
            // ideally we'd like to re-use the current parser objectmapper and context,
            // but I could not find a way to do so...
            ObjectReader objectReader = new ObjectMapper().reader()
                    .forType(SubgraphDefinition.class)
                    .withAttribute(PREFIXES_ATTRIBUTE, prefixes)
                    .withAttribute(VALUE_FACTORY_ATTRIBUTE, ctxt.getAttribute(VALUE_FACTORY_ATTRIBUTE));

            List<SubgraphDefinition> defs = new ArrayList<>();
            for (JsonNode node: subgraphs) {
                defs.add(objectReader.readValue(node));
            }

            return new SubgraphDefinitions(Collections.unmodifiableMap(prefixes), Collections.unmodifiableList(defs));
        }
    }

    abstract static class Deserializer<E> extends JsonDeserializer<E> {

        @Override
        public E deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            ValueFactory valueFactory = (ValueFactory) ctx.getAttribute(VALUE_FACTORY_ATTRIBUTE);
            if (valueFactory == null) {
                throw new IllegalStateException("Attribute " + VALUE_FACTORY_ATTRIBUTE + " not found");
            }
            Map<String, String> prefixes = (Map<String, String>) ctx.getAttribute(PREFIXES_ATTRIBUTE);
            if (prefixes == null) {
                throw new IllegalStateException("Attribute " + PREFIXES_ATTRIBUTE + " not found");
            }
            return deserialize(p, valueFactory, prefixes);
        }

        public abstract E deserialize(JsonParser p, ValueFactory valueFactory, Map<String, String> prefixes) throws IOException;
    }
}
