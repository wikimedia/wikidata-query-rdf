package org.wikidata.query.rdf.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

public class WikibaseUrisUnitTest {

    @Test
    public void defaultUris() {
        WikibaseUris uris = WikibaseUris.getURISystem();
        assertThat(uris.entityURIs()).contains("http://www.wikidata.org/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://www.wikidata.org/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://www.wikidata.org/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://www.wikidata.org/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://www.wikidata.org/wiki/Special:EntityData/");
    }

    @Test
    public void fromHost() {
        WikibaseUris uris = WikibaseUris.forHost("acme.test");
        assertThat(uris.entityURIs()).contains("http://acme.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme.test/wiki/Special:EntityData/");
    }

    @Test
    public void conceprUri() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("http://acme.test/prefix"));
        assertThat(uris.entityURIs()).contains("http://acme.test/prefix/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme.test/prefix/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme.test/prefix/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme.test/prefix/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme.test/prefix/wiki/Special:EntityData/");
    }

    @Test
    public void conceprUriHttps() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("https://acme2.test"));
        assertThat(uris.entityURIs()).contains("https://acme2.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("https://acme2.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("https://acme2.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("https://acme2.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("http://acme2.test/wiki/Special:EntityData/");
    }

    @Test
    public void conceprUriSlash() throws URISyntaxException {
        WikibaseUris uris = new WikibaseUris(new URI("http://acme3.test/"));
        assertThat(uris.entityURIs()).contains("http://acme3.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme3.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme3.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme3.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme3.test/wiki/Special:EntityData/");
    }

}
