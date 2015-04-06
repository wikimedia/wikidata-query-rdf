package org.wikidata.query.rdf.tool;

import java.io.IOException;

import org.junit.Test;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.EntityData;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.tool.rdf.Munger;

import com.google.common.io.Resources;

/**
 * Tests the munger that loads dumps.
 */
public class MungeIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {
    public MungeIntegrationTest() {
        super(new EntityData("test.wikidata.org"), new Entity("test.wikidata.org"));
    }

    /**
     * Loads a truncated version of a test dump from test wikidata.
     */
    @Test
    public void loadTest() throws IOException {
        String source = Resources.getResource(MungeIntegrationTest.class, "test.ttl").toString();
        Munge.Httpd http = new Munge.Httpd(10999, entityDataUris, entityUris,
                new Munger(entityDataUris, entityUris).singleLabelMode("en"), source);
        http.start();
        try {
            assertEquals(907, rdfRepository.loadUrl("http://localhost:10999"));
        } finally {
            http.stop();
        }
        assertTrue(rdfRepository.ask(RDFS.prefixes(entityUris.prefixes(new StringBuilder()))
                .append("ASK { entity:Q10 rdfs:label \"Wikidata\"@en }").toString()));
        assertTrue(rdfRepository.ask(SchemaDotOrg.prefix(Ontology.prefix(new StringBuilder()))
                .append("ASK { ontology:Dump schema:dateModified \"2015-04-02T10:54:56Z\"^^xsd:dateTime }").toString()));
    }
}
