package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URL;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Will fail if there isn't a local Blazegraph instance running on port 9999.
 * Maven will run Blzegraph on that port during the pre-integration-test phase
 * but if it fails to start then it won't always notice.
 */
public class BlazegraphCanaryIntegrationTest {
    @Test
    public void canary() throws IOException {
        String fetched = Resources.asCharSource(new URL("http://localhost:9999/bigdata/status"), Charsets.UTF_8).read();
        assertThat(fetched, containsString("queriesPerSecond"));
    }
}
