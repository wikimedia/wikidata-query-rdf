package org.wikidata.query.rdf.blazegraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.blazegraph.ProxiedHttpConnectionFactory.getListOfExcludedHosts;

import org.junit.Test;

public class ProxiedHttpConnectionFactoryUnitTest {
    @Test
    public void testExcludedHosts() {
        assertThat(getListOfExcludedHosts(null)).isEmpty();
        assertThat(getListOfExcludedHosts("")).isEmpty();
        assertThat(getListOfExcludedHosts(", ")).isEmpty();
        assertThat(getListOfExcludedHosts("foo,bar")).containsExactly("foo", "bar");
        assertThat(getListOfExcludedHosts("foo,,bar")).containsExactly("foo", "bar");
        assertThat(getListOfExcludedHosts(" foo , ,bar , ")).containsExactly("foo", "bar");
    }
}
