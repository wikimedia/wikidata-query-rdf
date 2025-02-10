package org.wikidata.query.rdf.common.uri;


import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;

import org.junit.Test;

public class UrisSchemeFactoryUnitTest {
    @Test
    public void defaultWikidataScheme() {
        DefaultUrisScheme scheme = UrisSchemeFactory.WIKIDATA;
        assertThat(scheme.entityData()).isEqualTo("http://www.wikidata.org/wiki/Special:EntityData/");
        assertThat(scheme.entityDataHttps()).isEqualTo("https://www.wikidata.org/wiki/Special:EntityData/");
        assertThat(scheme.entityIdToURI("Q2")).isEqualTo("http://www.wikidata.org/entity/Q2");
        assertThat(scheme.entityIdToURI("P2")).isEqualTo("http://www.wikidata.org/entity/P2");
        assertThat(scheme.entityIdToURI("L2")).isEqualTo("http://www.wikidata.org/entity/L2");
        assertThat(scheme.entityIdToURI("X2")).isEqualTo("http://www.wikidata.org/entity/X2");
        assertThat(scheme.inlinableEntityInitials()).containsExactly("P", "Q");
        assertThat(scheme.wellKnownBNodeIRIPrefix()).isEqualTo("http://www.wikidata.org/.well-known/genid/");
    }

    @Test
    public void defaultCommonsScheme() {
        UrisScheme scheme = UrisSchemeFactory.COMMONS;
        assertThat(scheme.entityData()).isEqualTo("https://commons.wikimedia.org/wiki/Special:EntityData/");
        assertThat(scheme.entityDataHttps()).isEqualTo("https://commons.wikimedia.org/wiki/Special:EntityData/");
        assertThat(scheme.entityIdToURI("Q2")).isEqualTo("http://www.wikidata.org/entity/Q2");
        assertThat(scheme.entityIdToURI("P2")).isEqualTo("http://www.wikidata.org/entity/P2");
        assertThat(scheme.entityIdToURI("L2")).isEqualTo("http://www.wikidata.org/entity/L2");
        assertThat(scheme.entityIdToURI("X2")).isEqualTo("http://www.wikidata.org/entity/X2");
        assertThat(scheme.entityIdToURI("M2")).isEqualTo("https://commons.wikimedia.org/entity/M2");
        assertThat(scheme.inlinableEntityInitials()).containsExactly("P", "Q", "M");
        assertThat(scheme.wellKnownBNodeIRIPrefix()).isEqualTo("https://commons.wikimedia.org/.well-known/genid/");
    }

    @Test
    public void testCustomFederated() {
        UrisScheme scheme = new FederatedUrisScheme(
                UrisSchemeFactory.forCommons(URI.create("https://test-commons.wikimedia.org")),
                UrisSchemeFactory.forWikidata(URI.create("http://test.wikidata.org")));
        assertThat(scheme.entityData()).isEqualTo("https://test-commons.wikimedia.org/wiki/Special:EntityData/");
        assertThat(scheme.entityDataHttps()).isEqualTo("https://test-commons.wikimedia.org/wiki/Special:EntityData/");
        assertThat(scheme.entityIdToURI("Q2")).isEqualTo("http://test.wikidata.org/entity/Q2");
        assertThat(scheme.entityIdToURI("P2")).isEqualTo("http://test.wikidata.org/entity/P2");
        assertThat(scheme.entityIdToURI("L2")).isEqualTo("http://test.wikidata.org/entity/L2");
        assertThat(scheme.entityIdToURI("X2")).isEqualTo("http://test.wikidata.org/entity/X2");
        assertThat(scheme.entityIdToURI("M2")).isEqualTo("https://test-commons.wikimedia.org/entity/M2");
        assertThat(scheme.inlinableEntityInitials()).containsExactly("P", "Q", "M");
        assertThat(scheme.wellKnownBNodeIRIPrefix()).isEqualTo("https://test-commons.wikimedia.org/.well-known/genid/");
    }
}
