package org.wikidata.query.rdf.tool.rdf.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AskQueryResponseUnitTest {

    @Mock private ContentResponse response;

    private final AskQueryResponse responseHandler = new AskQueryResponse();

    @Test
    public void trueIsParsed() throws IOException {
        setupResponse(
                "{\n" +
                "  \"head\" : { },\n" +
                "  \"boolean\" : true\n" +
                "}");

        Boolean parsed = responseHandler.parse(response);

        assertTrue(parsed);
    }

    @Test
    public void falseIsParsed() throws IOException {
        setupResponse(
                "{\n" +
                "  \"head\" : { },\n" +
                "  \"boolean\" : false\n" +
                "}");

        Boolean parsed = responseHandler.parse(response);

        assertFalse(parsed);
    }

    @Test(expected = IOException.class)
    public void invalidValueRaisesException() throws IOException {
        setupResponse(
                "{\n" +
                "  \"head\" : { },\n" +
                "  \"boolean\" : yes\n" +
                "}");

        responseHandler.parse(response);
    }

    @Test(expected = IOException.class)
    public void invalidJSONRaisesException() throws IOException {
        setupResponse("non json response");

        responseHandler.parse(response);
    }

    private void setupResponse(String content) {
        when(response.getContentAsString())
                .thenReturn(content);
    }

}
