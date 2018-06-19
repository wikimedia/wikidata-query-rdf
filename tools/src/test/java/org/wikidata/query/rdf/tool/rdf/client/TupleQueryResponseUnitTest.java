package org.wikidata.query.rdf.tool.rdf.client;

import static com.google.common.io.Resources.getResource;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.google.common.io.ByteStreams;

@RunWith(MockitoJUnitRunner.class)
public class TupleQueryResponseUnitTest {

    @Mock private ContentResponse response;

    private final TupleQueryResponse responseHandler = new TupleQueryResponse();

    @Test
    public void canParseTuples() throws IOException, QueryEvaluationException {
        byte[] bytes = loadResponseFromFile();
        setupResponse(bytes);

        TupleQueryResult parsed = responseHandler.parse(response);

        assertThat(parsed, is(notNullValue()));
        assertThat(parsed.getBindingNames(), hasSize(1));
        assertThat(parsed.getBindingNames(), contains("cause"));

        BindingSet binding = parsed.next();
        Binding cause = binding.getBinding("cause");

        assertThat(cause.getValue().stringValue(), is(equalTo("http://www.wikidata.org/entity/Q1347065")));

        binding = parsed.next();
        cause = binding.getBinding("cause");

        assertThat(cause.getValue().stringValue(), is(equalTo("http://www.wikidata.org/entity/Q3827083")));
    }

    @Test(expected = RuntimeException.class)
    public void failOnInvalidResponse() throws IOException {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        setupResponse(bytes);

        responseHandler.parse(response);
    }

    /**
     * loads the result of:
     *
     * SELECT * WHERE {
     *   wd:Q23 p:P509 _:b1.
     *   _:b1 ps:P509 ?cause.
     * }
     */
    private byte[] loadResponseFromFile() throws IOException {
        URL r = getResource(TupleQueryResponseUnitTest.class, "tuples-query.bin");
        return ByteStreams.toByteArray(r.openStream());
    }

    private void setupResponse(byte[] content) {
        when(response.getContent())
                .thenReturn(content);
    }
}
