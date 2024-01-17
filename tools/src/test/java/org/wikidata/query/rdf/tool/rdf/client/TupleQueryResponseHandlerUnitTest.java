package org.wikidata.query.rdf.tool.rdf.client;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.google.common.io.ByteStreams;

@RunWith(Parameterized.class)
public class TupleQueryResponseHandlerUnitTest {

    @Rule public MockitoRule rule = MockitoJUnit.rule();
    @Mock private ContentResponse response;

    private final TupleQueryResponseHandler responseHandler;

    private final String testResource;
    private final String expectedAcceptHeader;

    public TupleQueryResponseHandlerUnitTest(TupleQueryResponseHandler.ResponseFormat format, String testResource, String expectedAcceptHeader) {
        this.responseHandler = new TupleQueryResponseHandler(format);
        this.testResource = testResource;
        this.expectedAcceptHeader = expectedAcceptHeader;
    }
    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return Arrays.asList(
                new Object[]{TupleQueryResponseHandler.ResponseFormat.BINARY, "tuples-query.bin", "application/x-binary-rdf-results-table"},
                new Object[]{TupleQueryResponseHandler.ResponseFormat.JSON, "tuples-query.json", "application/json"}
        );
    }

    @Test
    public void canParseTuples() throws IOException, QueryEvaluationException {
        byte[] bytes = loadResponseFromFile(testResource);
        setupResponse(bytes);

        TupleQueryResult parsed = responseHandler.parse(response);

        assertThat(parsed).isNotNull();
        assertThat(parsed.getBindingNames()).hasSize(1);
        assertThat(parsed.getBindingNames()).contains("cause");

        BindingSet binding = parsed.next();
        Binding cause = binding.getBinding("cause");

        assertThat(cause.getValue().stringValue()).isEqualTo("http://www.wikidata.org/entity/Q1347065");

        binding = parsed.next();
        cause = binding.getBinding("cause");

        assertThat(cause.getValue().stringValue()).isEqualTo("http://www.wikidata.org/entity/Q3827083");
    }

    @Test(expected = RuntimeException.class)
    public void failOnInvalidResponse() throws IOException {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        setupResponse(bytes);

        responseHandler.parse(response);
    }

    @Test
    public void testAcceptHeader() {
        assertThat(responseHandler.acceptHeader()).isEqualTo(expectedAcceptHeader);
    }

    /**
     * loads the result of:
     *
     * SELECT * WHERE {
     *   wd:Q23 p:P509 _:b1.
     *   _:b1 ps:P509 ?cause.
     * }
     */
    private byte[] loadResponseFromFile(String resource) throws IOException {
        URL r = getResource(TupleQueryResponseHandlerUnitTest.class, resource);
        return ByteStreams.toByteArray(r.openStream());
    }

    private void setupResponse(byte[] content) {
        when(response.getContent())
                .thenReturn(content);
    }
}
