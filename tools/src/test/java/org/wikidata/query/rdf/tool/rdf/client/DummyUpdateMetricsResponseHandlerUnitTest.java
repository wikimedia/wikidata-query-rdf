package org.wikidata.query.rdf.tool.rdf.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;

public class DummyUpdateMetricsResponseHandlerUnitTest {

    private DummyUpdateMetricsResponseHandler handler;

    @Before
    public void setUp() {
        handler = new DummyUpdateMetricsResponseHandler();
    }

    @Test
    public void testAcceptHeader() {
        assertNull(handler.acceptHeader());
    }

    @Test
    public void testParseCollectsEmptyMetrics() throws IOException {
        ContentResponse response = mock(ContentResponse.class);
        CollectedUpdateMetrics metrics = handler.parse(response);
        // We expect to receive a non-null CollectedUpdateMetrics,
        // with counters and gauges initialized to zero.
        assertNotNull(metrics);
        assertEquals(0, metrics.getMutationCount());
        assertEquals(0, metrics.getCommitTotalElapsed());
    }
}
