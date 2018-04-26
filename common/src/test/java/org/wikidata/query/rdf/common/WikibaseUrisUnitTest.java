package org.wikidata.query.rdf.common;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.carrotsearch.randomizedtesting.RandomizedTest;

public class WikibaseUrisUnitTest extends RandomizedTest {

    @Test
    public void defaultUris() {
        WikibaseUris uris = WikibaseUris.getURISystem();
        assertEquals("http://www.wikidata.org/entity/", uris.entity());
        assertEquals("http://www.wikidata.org/wiki/Special:EntityData/", uris.entityData());
        assertEquals("https://www.wikidata.org/wiki/Special:EntityData/", uris.entityDataHttps());
    }

    @Test
    public void fromHost() {
        WikibaseUris uris = WikibaseUris.forHost("acme.test");
        assertEquals("http://acme.test/entity/", uris.entity());
        assertEquals("http://acme.test/wiki/Special:EntityData/", uris.entityData());
        assertEquals("https://acme.test/wiki/Special:EntityData/", uris.entityDataHttps());
    }

    @Test
    public void conceprUri() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("http://acme.test/prefix"));
        assertEquals("http://acme.test/prefix/entity/", uris.entity());
        assertEquals("http://acme.test/prefix/wiki/Special:EntityData/", uris.entityData());
        assertEquals("https://acme.test/prefix/wiki/Special:EntityData/", uris.entityDataHttps());
    }

    @Test
    public void conceprUriHttps() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("https://acme2.test"));
        assertEquals("https://acme2.test/entity/", uris.entity());
        assertEquals("https://acme2.test/wiki/Special:EntityData/", uris.entityData());
        assertEquals("http://acme2.test/wiki/Special:EntityData/", uris.entityDataHttps());
    }

    @Test
    public void conceprUriSlash() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("http://acme3.test/"));
        assertEquals("http://acme3.test/entity/", uris.entity());
        assertEquals("http://acme3.test/wiki/Special:EntityData/", uris.entityData());
        assertEquals("https://acme3.test/wiki/Special:EntityData/", uris.entityDataHttps());
    }

}
