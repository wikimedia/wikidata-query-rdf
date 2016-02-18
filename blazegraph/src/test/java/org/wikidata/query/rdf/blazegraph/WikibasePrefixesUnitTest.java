package org.wikidata.query.rdf.blazegraph;

import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import static org.wikidata.query.rdf.test.Matchers.assertResult;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.hamcrest.Matchers.both;

public class WikibasePrefixesUnitTest extends AbstractRandomizedBlazegraphTestBase {

    @Test
    public void testWikibasePrefixes() {
        add("ontology:dummy", "ontology:dummy", "wd:Q123");
        TupleQueryResult res = query("SELECT * WHERE { wikibase:dummy ?x ?y }");
        assertResult(res, both(
                             binds("x", new URIImpl(Ontology.NAMESPACE + "dummy"))
                          ).and(
                             binds("y", new URIImpl(uris().entity() + "Q123"))
                    ));

        TupleQueryResult res2 = query("SELECT * WHERE { ?x ?y wd:Q123 }");
        assertResult(res2, binds("x", new URIImpl(Ontology.NAMESPACE + "dummy")));
    }

    @Test
    public void testPrefixesRFDSandSchema() {
        add("wd:Q123", SchemaDotOrg.ABOUT, SKOS.ALT_LABEL);
        TupleQueryResult res = query("SELECT * WHERE { ?x schema:about skos:altLabel }");
        assertResult(res, binds("x", new URIImpl(uris().entity() + "Q123")));
    }

}
