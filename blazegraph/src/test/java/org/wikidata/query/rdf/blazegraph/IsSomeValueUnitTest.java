package org.wikidata.query.rdf.blazegraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.Matchers.binds;

import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.model.BigdataURI;

public class IsSomeValueUnitTest extends AbstractBlazegraphTestBase {
    @Test
    public void testReturnTrueOnBNode() throws QueryEvaluationException {
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnTrueOnBNode");
        add(uri, uri, store().getValueFactory().createBNode());
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isTrue();
        BindingSet result = tqr.next();
        assertThat(result).matches((e) -> binds("s", uri).matches(e));
    }

    @Test
    public void testReturnFalseOnConcreteValue() throws QueryEvaluationException {
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnFalseOnConcreteValue");
        add(uri, uri, uri);
        add(uri, uri, store().getValueFactory().createLiteral("foo"));
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isFalse();
    }
}
