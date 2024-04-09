package org.wikidata.query.rdf.tool.subgraph;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Value;

@Value
@JsonDeserialize(using = SubgraphRule.Deserializer.class)
public class SubgraphRule {
    private static final Pattern RULE_PATTERN = Pattern.compile("^(pass|block)\\s+(\\S.+)$");
    Outcome outcome;
    TriplePattern pattern;


    public enum Outcome {
        pass, block
    }

    public static class Deserializer extends SubgraphDefinitions.Deserializer<SubgraphRule> {
        @Override
        public SubgraphRule deserialize(JsonParser p, ValueFactory valueFactory, Map<String, String> prefixes) throws IOException {
            JsonToken token = p.currentToken();
            if (token != JsonToken.VALUE_STRING) {
                throw new JsonParseException(p, "Expected a string but got " + token);
            }
            return parse(p.getValueAsString(), valueFactory, prefixes);
        }

        private SubgraphRule parse(String def, ValueFactory valueFactory, Map<String, String> prefixes) {
            Matcher m = RULE_PATTERN.matcher(def);

            if (!m.find()) {
                throw new IllegalArgumentException("Invalid rule definition: " + def);
            }

            return new SubgraphRule(
                    Outcome.valueOf(m.group(1)),
                    TriplePattern.parseTriplePattern(m.group(2), valueFactory, prefixes)
            );
        }
    }

    @Value
    public static class TriplePattern {
        public static final String ENTITY_BINDING_NAME = "entity";
        public static final String WILDCARD_BNODE_LABEL = "wildcard";
        public static final String WILDCARD = "[]";

        private static final Pattern TRIPLE_PATTERN_PATTERN = Pattern.compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s*$");

        Resource subject;
        URI predicate;
        Resource object;

        public static TriplePattern parseTriplePattern(String expression, ValueFactory valueFactory, Map<String, String> prefixes) {
            Matcher m = TRIPLE_PATTERN_PATTERN.matcher(expression);

            if (!m.find()) {
                throw new IllegalArgumentException("Invalid rule definition: " + expression);
            }

            return new TriplePattern(
                    parseValue(m.group(1), valueFactory, prefixes),
                    SubgraphDefinitions.parseUri(m.group(2), valueFactory, prefixes),
                    parseValue(m.group(3), valueFactory, prefixes)
            );
        }

        private static Resource parseValue(String value, ValueFactory valueFactory, Map<String, String> prefixes) {
            if (value.startsWith("?")) {
                if (!value.equals("?" + ENTITY_BINDING_NAME)) {
                    throw new IllegalArgumentException("Unsupported binding " + value);
                }
                return valueFactory.createBNode(ENTITY_BINDING_NAME);
            }
            if (value.equals(WILDCARD)) {
                return valueFactory.createBNode(WILDCARD_BNODE_LABEL);
            }
            return SubgraphDefinitions.parseUri(value, valueFactory, prefixes);
        }
    }
}
