package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.blank;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Test;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.helpers.StatementCollector;

public class RDFParserSuppliersUnitTest {
    @Test
    public void testBNodePreserved() throws RDFParseException, IOException, RDFHandlerException {
        StatementCollector collector = new StatementCollector();
        RDFParser parser = RDFParserSuppliers.defaultRdfParser().get(collector);
        parser.parse(new StringReader("<uri:a> <uri:b> _:123 ."), "unused");
        assertThat(collector.getStatements()).containsOnly(statement("uri:a", "uri:b", blank("123")));
    }
}
