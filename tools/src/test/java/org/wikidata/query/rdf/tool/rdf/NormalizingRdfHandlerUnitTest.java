package org.wikidata.query.rdf.tool.rdf;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;

import static org.junit.Assert.assertEquals;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

public class NormalizingRdfHandlerUnitTest {

    @Test
    public void testHandleVersion001() throws RDFHandlerException {
        testHandleStatement("0.0.1");
    }

    @Test
    public void testHandleVersionBeta() throws RDFHandlerException {
        testHandleStatement("beta");
    }

    private void testHandleStatement(String suffix) throws RDFHandlerException {
        StatementChecker checkStatement = new StatementChecker();
        NormalizingRdfHandler handler = new NormalizingRdfHandler(checkStatement);
        String testStr = "http://www.wikidata.org/ontology-" + suffix + "#Test";

        Statement s = statement(testStr, RDF.TYPE, Ontology.ITEM);
        Statement p = statement("Q1", testStr, Ontology.ITEM);
        Statement o = statement("Q1", RDF.TYPE, new URIImpl(testStr));
        Statement o2 = statement("Q1", RDF.TYPE, new LiteralImpl(testStr));

        checkStatement.expect("http://wikiba.se/ontology#Test", null, null);
        handler.handleStatement(s);
        checkStatement.expect(null, "http://wikiba.se/ontology#Test", null);
        handler.handleStatement(p);
        checkStatement.expect(null, null, "http://wikiba.se/ontology#Test");
        handler.handleStatement(o);
        checkStatement.expect(null, null, testStr);
        handler.handleStatement(o2);
    }

    @Test
    public void testHandleNamespace() throws RDFHandlerException {
        StatementChecker checkStatement = new StatementChecker();
        NormalizingRdfHandler handler = new NormalizingRdfHandler(checkStatement);

        checkStatement.expectURI("http://wikiba.se/ontology#Test");
        handler.handleNamespace("test", "http://www.wikidata.org/ontology-0.0.1#Test");
        handler.handleNamespace("test", "http://www.wikidata.org/ontology-beta#Test");
    }

    @Test
    public void testHandleBadChars() throws RDFHandlerException {
        StatementChecker checkStatement = new StatementChecker();
        NormalizingRdfHandler handler = new NormalizingRdfHandler(checkStatement);
        Statement s = statement("Q1", RDF.TYPE, "http://viaf.org/processed\\BNC|a10474614`^{}\n");
        checkStatement.expect(null, null, "http://viaf.org/processed%5CBNC%7Ca10474614%60%5E%7B%7D");
        handler.handleStatement(s);
    }

    @Test
    public void testHandleBadDecimal() throws RDFHandlerException {
        StatementChecker checkStatement = new StatementChecker();
        NormalizingRdfHandler handler = new NormalizingRdfHandler(checkStatement);
        handler.handleNamespace("xsd", XMLSchema.NAMESPACE);
        Statement s = statement("Q1", "P1", new LiteralImpl("", XMLSchema.DECIMAL));
        checkStatement.expect(null, null, "0");
        handler.handleStatement(s);
    }

    private final class StatementChecker implements RDFHandler {
        private String expectSubject;
        private String expectPredicate;
        private String expectObject;
        private String expectURI;

        public void expect(String s, String p, String o) {
            expectSubject = s;
            expectPredicate = p;
            expectObject = o;
        }

        public void expectURI(String u) {
            expectURI = u;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void endRDF() throws RDFHandlerException {

        }

        @Override
        public void handleNamespace(String prefix, String uri) {
            if (expectURI != null) {
                assertEquals(expectURI, uri);
            }
        }

        @Override
        public void handleStatement(Statement st) {
            if (expectSubject != null) {
                assertEquals(expectSubject, st.getSubject().stringValue());
            }
            if (expectPredicate != null) {
                assertEquals(expectPredicate, st.getPredicate().stringValue());
            }
            if (expectObject != null) {
                assertEquals(expectObject, st.getObject().stringValue());
            }
        }

        @Override
        public void handleComment(String comment) {
        }

    }
}
