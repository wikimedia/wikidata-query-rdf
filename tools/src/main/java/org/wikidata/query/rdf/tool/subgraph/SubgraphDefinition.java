package org.wikidata.query.rdf.tool.subgraph;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Class representing a subgraph.
 */
@Value
// For some reason, the implicit @AllArgsConstructor does not work with jackson under scala
// @Jacksonized provides a workaround via @Builder + @JsonDeserialize
@Builder
@Jacksonized
public class SubgraphDefinition implements Serializable {

    /** Name of the subgraph. */
    @JsonProperty
    String name;

    /** URI of the subgraph, used to generate stubs. */
    @Nullable
    @JsonProperty("subgraph_uri") @JsonDeserialize(using = UriDeserializer.class)
    URI subgraphUri;

    /** EventStream name of the subgraph for realtime updates. */
    @JsonProperty
    String stream;

    /** List of rules. */
    @Nullable
    @JsonProperty
    List<SubgraphRule> rules;

    /** Default outcome to apply when no rules are matching. */
    @JsonProperty("default")
    SubgraphRule.Outcome ruleDefault;

    /** Determine if this graph can be referenced in stubs. */
    @JsonProperty("stubs_source")
    boolean stubsSource;

    static class UriDeserializer extends SubgraphDefinitions.Deserializer<URI> {
        @Override
        public URI deserialize(JsonParser p, ValueFactory valueFactory, Map<String, String> prefixes,
                               Map<String, Collection<Resource>> bindings) throws IOException {
            JsonToken token = p.currentToken();
            if (token != JsonToken.VALUE_STRING) {
                throw new JsonParseException(p, "Expected a string but got " + token);
            }
            return SubgraphDefinitions.parseUri(p.getValueAsString(), valueFactory, prefixes);
        }
    }
}
