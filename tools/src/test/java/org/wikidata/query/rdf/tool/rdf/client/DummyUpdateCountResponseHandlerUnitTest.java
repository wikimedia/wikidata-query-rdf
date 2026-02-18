package org.wikidata.query.rdf.tool.rdf.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Before;
import org.junit.Test;

public class DummyUpdateCountResponseHandlerUnitTest {

    private DummyUpdateCountResponseHandler handler;

    @Before
    public void setUp() {
        handler = new DummyUpdateCountResponseHandler();
    }

    @Test
    public void testAcceptHeader() {
        assertNull(handler.acceptHeader());
    }

    @Test
    public void testParseCollectsEmptyMetrics() throws IOException {
        ContentResponse response = mock(ContentResponse.class);
        Integer counter = handler.parse(response);
        // We expect to receive a non-null counter initialized to zero.
        assertNotNull(counter);
        assertEquals(Integer.valueOf(0), counter);
    }
}
