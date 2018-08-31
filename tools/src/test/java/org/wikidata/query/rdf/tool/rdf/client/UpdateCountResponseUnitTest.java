package org.wikidata.query.rdf.tool.rdf.client;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.io.ByteSource;

@RunWith(MockitoJUnitRunner.class)
public class UpdateCountResponseUnitTest {

    @Mock private ContentResponse response;

    private final UpdateCountResponse responseHandler = new UpdateCountResponse();

    @Test
    public void canParseUpdateCounts() throws IOException {
        String content = loadResponseFromFile();
        setupResponse(content);

        Integer numberOfUpdates = responseHandler.parse(response);

        assertThat(numberOfUpdates).isEqualTo(4);
    }

    private String loadResponseFromFile() throws IOException {
        URL r = getResource(TupleQueryResponseUnitTest.class, "update-response.html");

        ByteSource source = new ByteSource() {
            @Override
            public InputStream openStream() throws IOException {
                return r.openStream();
            }
        };

        return source.asCharSource(UTF_8).read();
    }

    private void setupResponse(String content) {
        when(response.getContentAsString())
                .thenReturn(content);
    }
}
