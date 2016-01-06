package org.wikidata.query.rdf.blazegraph;

import java.math.BigInteger;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;

/**
 * Base class for tests that need to interact with a temporary triple store. All
 * the triple store creation logic lives in the parent class. This class just
 * has convenient utilities to help with things like adding data and running
 * queries.
 */
public class AbstractRandomizedBlazegraphTestBase extends AbstractRandomizedBlazegraphStorageTestCase {
    /**
     * Which uris this test uses.
     */
    private WikibaseUris uris = WikibaseUris.WIKIDATA;

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
            s = s.replaceFirst("^wd:", uris.entity());
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

    /*
     * Initialize the Wikibase services including shutting off remote SERVICE
     * calls and turning on label service calls.
     */
    static {
        WikibaseContextListener.initializeServices();
        System.setProperty("ASTOptimizerClass", WikibaseOptimizers.class.getName());
    }
}
