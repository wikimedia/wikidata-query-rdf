package org.wikidata.query.rdf.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_INITIALS;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.DefaultUrisScheme;

public class DefaultUrisSchemeUnitTest {

    @Test
    public void defaultUris() {
        UrisScheme uris = UrisSchemeFactory.getURISystem();
        assertThat(uris.entityURIs()).contains("http://www.wikidata.org/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://www.wikidata.org/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://www.wikidata.org/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://www.wikidata.org/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://www.wikidata.org/wiki/Special:EntityData/");
    }

    @Test
    public void fromHost() {
        UrisScheme uris = UrisSchemeFactory.forWikidataHost("acme.test");
        assertThat(uris.entityURIs()).contains("http://acme.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme.test/wiki/Special:EntityData/");
    }

    @Test
    public void conceptUri() throws URISyntaxException {
        UrisScheme uris = new DefaultUrisScheme(new URI("http://acme.test/prefix"), WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        assertThat(uris.entityURIs()).contains("http://acme.test/prefix/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme.test/prefix/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme.test/prefix/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme.test/prefix/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme.test/prefix/wiki/Special:EntityData/");
    }

    @Test
    public void conceptUriHttps() throws URISyntaxException {
        UrisScheme uris = new DefaultUrisScheme(new URI("https://acme2.test"), WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        assertThat(uris.entityURIs()).contains("https://acme2.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("https://acme2.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("https://acme2.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("https://acme2.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("http://acme2.test/wiki/Special:EntityData/");
    }

    @Test
    public void conceptUriSlash() throws URISyntaxException {
        UrisScheme uris = new DefaultUrisScheme(new URI("http://acme3.test/"), WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        assertThat(uris.entityURIs()).contains("http://acme3.test/entity/");
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme3.test/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme3.test/entity/P1");
        assertThat(uris.entityData()).isEqualTo("http://acme3.test/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme3.test/wiki/Special:EntityData/");
    }

    @Test
    public void initialsOrder() throws URISyntaxException {
        UrisScheme uris = new DefaultUrisScheme(new URI("https://acme2.test"), WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        // See https://phabricator.wikimedia.org/T230588 for reasons for this order
        assertThat(uris.inlinableEntityInitials()).containsExactly("P", "Q");
    }

    @Test
    public void testBNodeSkolemIRIPrefix() throws URISyntaxException {
        UrisScheme uris = new DefaultUrisScheme(new URI("http://acme2.test"), WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        assertThat(uris.wellKnownBNodeIRIPrefix()).isEqualTo("http://acme2.test/.well-known/genid/");
    }
}
