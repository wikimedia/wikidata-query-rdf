package org.wikidata.query.rdf.common.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.common.uri.UrisConstants.MEDIAINFO_INITIAL;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.SDC_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_DATA_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_ENTITY_PREFIX;
import static org.wikidata.query.rdf.common.uri.UrisConstants.WIKIBASE_INITIALS;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.common.uri.FederatedUrisScheme;
import org.wikidata.query.rdf.common.uri.DefaultUrisScheme;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.UrisScheme;

public class FederatedUrisSchemeUnitTest {
    private UrisScheme uris;

    @Before
    public void initURIs() throws URISyntaxException {
        final URI commonsUrl = new URI("http://acme.commons/something");
        final URI wikidataUrl = new URI("http://acme.test/prefix");
        DefaultUrisScheme sdcUris = new DefaultUrisScheme(commonsUrl, SDC_ENTITY_PREFIX, SDC_ENTITY_DATA_PREFIX, MEDIAINFO_INITIAL);
        DefaultUrisScheme wikibaseUris = new DefaultUrisScheme(wikidataUrl, WIKIBASE_ENTITY_PREFIX, WIKIBASE_ENTITY_DATA_PREFIX, WIKIBASE_INITIALS);
        uris = new FederatedUrisScheme(sdcUris, wikibaseUris);
    }

    @Test
    public void entityURIs() {
        assertThat(uris.entityURIs()).contains("http://acme.commons/something/entity/");
        assertThat(uris.entityURIs()).contains("http://acme.test/prefix/entity/");
    }

    @Test
    public void entityIdToURI() {
        assertThat(uris.entityIdToURI("Q1")).isEqualTo("http://acme.test/prefix/entity/Q1");
        assertThat(uris.entityIdToURI("P1")).isEqualTo("http://acme.test/prefix/entity/P1");
        assertThat(uris.entityIdToURI("M1")).isEqualTo("http://acme.commons/something/entity/M1");
    }

    @Test
    public void entityData() {
        assertThat(uris.entityData()).isEqualTo("http://acme.commons/something/wiki/Special:EntityData/");
        assertThat(uris.entityDataHttps()).isEqualTo("https://acme.commons/something/wiki/Special:EntityData/");
    }

    @Test
    public void isEntityURI() {
        assertThat(uris.isEntityURI("http://acme.test/prefix/entity/Q1")).isTrue();
        assertThat(uris.isEntityURI("http://acme.test/prefix/entity/P1")).isTrue();
        assertThat(uris.isEntityURI("http://acme.test/prefix/entity/L1")).isTrue();
        assertThat(uris.isEntityURI("http://acme.commons/something/entity/M1")).isTrue();
    }

    @Test
    public void entityURItoId() {
        assertThat(uris.entityURItoId("http://acme.test/prefix/entity/Q1")).isEqualTo("Q1");
        assertThat(uris.entityURItoId("http://acme.test/prefix/entity/P1")).isEqualTo("P1");
        assertThat(uris.entityURItoId("http://acme.commons/something/entity/M1")).isEqualTo("M1");
    }

    @Test
    public void entityPrefixes() {
        assertThat(uris.entityPrefixes()).containsEntry("wd", "http://acme.test/prefix/entity/");
        assertThat(uris.entityPrefixes()).containsEntry("sdc", "http://acme.commons/something/entity/");
    }

    @Test
    public void entityInitials() {
        assertThat(uris.entityInitials()).contains("Q", "P", "M");
    }

    @Test
    public void testSomeValuePrefix() {
        assertThat(uris.wellKnownBNodeIRIPrefix()).isEqualTo("http://acme.commons/.well-known/genid/");
    }

    @Test
    public void testCorrectPropertyPrefixes() {
        assertThat(uris.property(PropertyType.CLAIM)).isEqualTo("http://acme.test/prefix/prop/");
        assertThat(uris.property("direct/")).isEqualTo("http://acme.test/prefix/prop/direct/");
    }
}
