package org.wikidata.query.rdf.blazegraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.blazegraph.WikibaseContextListener.registerAllowedFederatedService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class WikibaseContextListenerUnitTest {
    @Test
    public void testRegisterAllowedFederatedService() {
        Map<URI, URI> aliases = new HashMap<>();
        Set<String> allowlist = new HashSet<>();
        registerAllowedFederatedService("http://one.unittest.local", allowlist::add, aliases::put);
        registerAllowedFederatedService(", http://internal.unittest.local, , http://expose.unittest.local , ", allowlist::add, aliases::put);
        assertThat(aliases)
                .hasSize(1)
                .containsEntry(new URIImpl("http://internal.unittest.local"), new URIImpl("http://expose.unittest.local"));
        assertThat(allowlist).containsOnly("http://one.unittest.local", "http://internal.unittest.local", "http://expose.unittest.local");
    }
}
