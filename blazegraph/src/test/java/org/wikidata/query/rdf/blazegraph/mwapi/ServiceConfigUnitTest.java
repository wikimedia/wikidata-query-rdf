package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;

@RunWith(RandomizedRunner.class)
public class ServiceConfigUnitTest extends RandomizedTest {

    private ServiceConfig loadFromFile(String filename) throws IOException {
        return new ServiceConfig(
                new InputStreamReader(
                        getClass().getClassLoader()
                                .getResourceAsStream("services/" + filename),
                        StandardCharsets.UTF_8));
    }

    @Test(expected = NullPointerException.class)
    public void testEmptyConfig() throws Exception {
        ServiceConfig config = loadFromFile("empty.json");
    }

    @Test
    public void testMinimalConfig() throws Exception {
        // Minimal empty config is not super-useful, but OK
        ServiceConfig config = loadFromFile("empty-config.json");
        assertEquals(config.size(), 0);
        assertFalse(config.validEndpoint("en.wikipedia.org"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoService() throws Exception {
        // Minimal empty config is not super-useful, but OK
        ServiceConfig config = loadFromFile("empty-config.json");
        assertNull(config.getService("Categories"));
        fail("getService did not throw");
    }

    @Test
    public void testConfig() throws Exception {
        ServiceConfig config = loadFromFile("services.json");
        assertEquals(config.size(), 1);
        assertThat(config.getService("Categories"), instanceOf(ApiTemplate.class));
        assertTrue(config.validEndpoint("en.wikipedia.org"));
        assertTrue(config.validEndpoint("ru.wikipedia.org"));
        assertTrue(config.validEndpoint("www.mediawiki.org"));
        assertFalse(config.validEndpoint("wikileaks.org"));
        assertFalse(config.validEndpoint("fakewikipedia.org"));
    }
}
