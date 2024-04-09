package org.wikidata.query.rdf.tool.subgraph;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Value;

/**
 * Class representing a subgraph.
 */
@Value
public class SubgraphDefinition {
    /** Name of the subgraph. */
    String name;

    /** URI of the subgraph, used to generate stubs. */
    @Nullable @JsonProperty("subgraph_uri") @JsonDeserialize(using = UriDeserializer.class)
    URI subgraphUri;

    /** EventStream name of the subgraph for realtime updates. */
    String stream;

    /** List of rules. */
    @Nullable
    List<SubgraphRule> rules;

    /** Default outcome to apply when no rules are matching. */
    @JsonProperty("default")
    SubgraphRule.Outcome ruleDefault;

    /** Determine if this graph can be referenced in stubs. */
    @JsonProperty("stubs_source")
    boolean stubsSource;

    static class UriDeserializer extends SubgraphDefinitions.Deserializer<URI> {
        @Override
        public URI deserialize(JsonParser p, ValueFactory valueFactory, Map<String, String> prefixes) throws IOException {
            JsonToken token = p.currentToken();
            if (token != JsonToken.VALUE_STRING) {
                throw new JsonParseException(p, "Expected a string but got " + token);
            }
            return SubgraphDefinitions.parseUri(p.getValueAsString(), valueFactory, prefixes);
        }
    }
}
