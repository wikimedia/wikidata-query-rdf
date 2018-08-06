package org.wikidata.query.rdf.tool.options;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.handleOptions;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

public class UpdateOptionsUnitTest {

    @Test
    public void test() throws URISyntaxException {
        UpdateOptions updateOptions = handleOptions(
                UpdateOptions.class,
                "--sparqlUrl", "http://test.net/sparql",
                "--wikibaseUrl", "http://test.net/wikibase");

        URI wikibaseUrl = UpdateOptions.getWikibaseUrl(updateOptions);

        assertThat(wikibaseUrl, is(equalTo(new URI("http://test.net/wikibase"))));
    }
}
