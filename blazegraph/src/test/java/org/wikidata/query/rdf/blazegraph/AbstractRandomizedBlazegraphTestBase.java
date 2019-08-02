package org.wikidata.query.rdf.blazegraph;

import java.math.BigInteger;
import java.util.Map;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;

/**
 * Base class for tests that need to interact with a temporary triple store. All
 * the triple store creation logic lives in the parent class. This class just
 * has convenient utilities to help with things like adding data and running
 * queries.
 */
public class AbstractRandomizedBlazegraphTestBase extends AbstractRandomizedBlazegraphStorageTestCase {
    private static WikibaseContextListener wikibaseContextListener;
    private static String astOptimizerClassBackup;
    /**
     * Which uris this test uses.
     */
    private WikibaseUris uris = WikibaseUris.getURISystem();

    /**
     * The uris this test uses.
     */
    protected WikibaseUris uris() {
        return uris;
    }

    /**
     * Run a query.
     */
    protected TupleQueryResult query(String query) {
        try {
            ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);

            return ASTEvalHelper.evaluateTupleQuery(store(), astContainer, new QueryBindingSet(), null);
        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    // Proxy Query Result class, which exposes ASTContainer to test framework so it might be logged in case of assertion failures
    public static final class QueryResult {
        final TupleQueryResult tqr;
        final ASTContainer ast;
        public QueryResult(TupleQueryResult tqr, ASTContainer astContainer) {
            this.tqr = tqr;
            this.ast = astContainer;
        }
        public ASTContainer ast() {
            return ast;
        }
        public boolean hasNext() throws QueryEvaluationException {
            return tqr.hasNext();
        }
        public BindingSet next() throws QueryEvaluationException {
            return tqr.next();
        }
    }
    /**
     * Run a query, returning both TupleQueryResult and ASTContainer.
     */
    protected QueryResult queryWithAST(String query) {
        try {
            ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);

            return new QueryResult(ASTEvalHelper.evaluateTupleQuery(store(), astContainer, new QueryBindingSet(), null), astContainer);
        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run an ASK query.
     */
    protected boolean ask(String query) {
        try {
            ASTContainer astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);

            return ASTEvalHelper.evaluateBooleanQuery(store(), astContainer, new QueryBindingSet(), null);
        } catch (MalformedQueryException | QueryEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a triple to the store.
     */
    protected void add(Object s, Object p, Object o) {
        store().addStatement((Resource) convert(s), (URI) convert(p), convert(o), null);
    }

    /**
     * Round trip a statement through Blazegraph.
     */
    protected BigdataStatement roundTrip(Object s, Object p, Object o) {
        return roundTrip((Resource) convert(s), (URI) convert(p), convert(o));
    }

    /**
     * Round trip a statement through Blazegraph.
     */
    protected BigdataStatement roundTrip(Resource s, URI p, Value o) {
        store().addStatement(s, p, o, null);
        return store().getStatement(s, p, o, null);
    }

    /**
     * Convert any object into an RDF value.
     */
    protected Value convert(Object o) {
        if (o instanceof Value) {
            return (Value) o;
        }
        if (o instanceof String) {
            String s = (String) o;
            s = s.replaceFirst("^ontology:", Ontology.NAMESPACE);
            s = s.replaceFirst("^wdata:", uris.entityData());
            for (Map.Entry<String, String> entry : uris.entityPrefixes().entrySet()) {
                s = s.replaceFirst("^" + entry.getKey() + ":", entry.getValue());
            }
            s = s.replaceFirst("^wds:", uris.statement());
            s = s.replaceFirst("^wdv:", uris.value());
            s = s.replaceFirst("^wdref:", uris.reference());
            for (PropertyType p : PropertyType.values()) {
                s = s.replaceFirst("^" + p.prefix() + ":", uris.property(p));
            }
            return new URIImpl(s);
        }
        if (o instanceof Integer) {
            return new IntegerLiteralImpl(BigInteger.valueOf((int) o));
        }
        throw new RuntimeException("No idea how to convert " + o + " to a value.  Its a " + o.getClass() + ".");
    }

    /**
     * Initialize the Wikibase services including shutting off remote SERVICE
     * calls and turning on label service calls.
     */
    static {
        wikibaseContextListener = new WikibaseContextListener();
        wikibaseContextListener.initializeServices();
        astOptimizerClassBackup = System.getProperty("ASTOptimizerClass");
        System.setProperty("ASTOptimizerClass", WikibaseOptimizers.class.getName());
    }

    /**
     * Create a constant node from string.
     * @param value
     * @return
     */
    protected TermNode createConstant(String value) {
        BigdataLiteral literal = store().getLexiconRelation().getValueFactory().createLiteral(value);
        return new DummyConstantNode(literal);
    }

    /**
     * Create a constant URI node from string.
     * @param value
     * @return
     */
    protected TermNode createURI(URI value) {
        BigdataURI uri = store().getLexiconRelation().getValueFactory().createURI(value.toString());
        return new DummyConstantNode(uri);
    }

    /**
     * Create a constant URI node from string.
     * @param value
     * @return
     */
    protected TermNode createURI(String value) {
        return createURI(new URIImpl(value));
    }
}
