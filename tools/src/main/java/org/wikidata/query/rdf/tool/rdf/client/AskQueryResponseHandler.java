package org.wikidata.query.rdf.tool.rdf.client;

import static org.wikidata.query.rdf.tool.MapperUtils.getObjectMapper;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses responses to ask queries into booleans.
 */
class AskQueryResponseHandler implements ResponseHandler<Boolean> {

    private final ObjectMapper mapper = getObjectMapper();

    @Override
    public String acceptHeader() {
        return "application/json";
    }

    @Override
    public Boolean parse(ContentResponse entity) throws IOException {
        try {
            return mapper.readValue(entity.getContentAsString(), Resp.class).aBoolean;
        } catch (JsonParseException | JsonMappingException e) {
            throw new IOException("Error parsing response", e);
        }
    }

    public static class Resp {
        private final Boolean aBoolean;

        @JsonCreator
        Resp(@JsonProperty("boolean") Boolean aBoolean) {
            this.aBoolean = aBoolean;
        }
    }
}
