package org.wikidata.query.rdf.tool.wikibase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.wikidata.query.rdf.tool.MapperUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class WikibaseResponseUnitTest {

    private final ObjectMapper mapper = MapperUtils.getObjectMapper();

    @Test
    public void stringErrorIsParsedCorrectly() throws IOException {
        MockResponse response = mapper.readValue(load("response_with_string_error.json"), MockResponse.class);
        assertThat(response.getError(), is(equalTo("some error")));
    }

    @Test
    public void complexErrorIsParsedCorrectly() throws IOException {
        MockResponse response = mapper.readValue(load("response_with_complex_error.json"), MockResponse.class);
        assertThat(
                response.getError(),
                is(equalTo(
                        ImmutableMap.of(
                                "code", 123,
                                "reason", "some reason"))));
    }

    @Test
    public void complexErrorIsDumpedToString() throws IOException {
        MockResponse response = mapper.readValue(load("response_with_complex_error.json"), MockResponse.class);
        assertThat(response.getError().toString(), is(equalTo("{code=123, reason=some reason}")));
    }

    private InputStream load(String name) {
        String prefix = this.getClass().getPackage().getName().replace(".", "/");
        return getClass().getClassLoader().getResourceAsStream(prefix + "/" + name);
    }

    private static final class MockResponse extends WikibaseBaseResponse {
        @JsonCreator
        MockResponse(
                @JsonProperty("error") Object error
        ) {
            super(error);
        }
    }

}
