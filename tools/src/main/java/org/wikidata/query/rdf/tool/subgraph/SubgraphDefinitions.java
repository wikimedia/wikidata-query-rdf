package org.wikidata.query.rdf.tool.subgraph;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Collection;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import lombok.Value;
import lombok.val;

@Value
public class SubgraphDefinitions {
    static final String PREFIXES_ATTRIBUTE = SubgraphDefinitions.class.getName() + "-prefixes";
    static final String VALUE_FACTORY_ATTRIBUTE = SubgraphDefinitions.class.getName() + "-value-factory";
    private static final Pattern PREFIX = Pattern.compile("^([a-zA-Z\\d]+):(.*)$");
    List<SubgraphDefinition> subgraphs;

    public SubgraphDefinitions(@JsonProperty("subgraphs") List<SubgraphDefinition> subgraphs) {
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
