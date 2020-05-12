package org.wikidata.query.rdf.blazegraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikidata.query.rdf.test.Matchers.binds;

import org.junit.AfterClass;
import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.blazegraph.constraints.IsSomeValueFunctionFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;

public class IsSomeValueUnitTest extends AbstractBlazegraphTestBase {
    public static final java.util.function.BiConsumer<org.openrdf.model.URI, FunctionRegistry.Factory> REPLACE_FUNC_IN_GLOBAL_REGISTRY = (u, f) -> {
        FunctionRegistry.remove(u);
        FunctionRegistry.add(u, f);
    };

    public static final UrisScheme scheme = UrisSchemeFactory.getURISystem();

    @AfterClass
    public static void after() {
        // sadly the function registry in blazegraph is entirely static
        // the other alternative is to not test the integration with Blazegraph
        switchToBlank();
    }

    static void switchToBlank() {
        WikibaseContextListener.registerIsSomeValueFunction(
                REPLACE_FUNC_IN_GLOBAL_REGISTRY,
                IsSomeValueFunctionFactory.SomeValueMode.Blank, null);
    }

    static void switchToSkolem() {
        WikibaseContextListener.registerIsSomeValueFunction(
                REPLACE_FUNC_IN_GLOBAL_REGISTRY,
                IsSomeValueFunctionFactory.SomeValueMode.Skolem, scheme.wellKnownBNodeIRIPrefix());
    }

    @Test
    public void testReturnTrueOnBNode() throws QueryEvaluationException {
        switchToBlank();
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnTrueOnBNode");
        add(uri, uri, store().getValueFactory().createBNode());
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isTrue();
        BindingSet result = tqr.next();
        assertThat(result).matches((e) -> binds("s", uri).matches(e));
    }

    @Test
    public void testReturnFalseOnConcreteValue() throws QueryEvaluationException {
        switchToBlank();
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnFalseOnConcreteValue");
        add(uri, uri, uri);
        add(uri, uri, store().getValueFactory().createLiteral("foo"));
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isFalse();
    }

    @Test
    public void testReturnTrueOnSkolemPrefix() throws QueryEvaluationException {
        switchToSkolem();
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnTrueOnSkolemPrefix");
        BigdataURI skolem = store().getValueFactory().createURI(scheme.wellKnownBNodeIRIPrefix(), "91212dc3fcc8b65607d27f92b36e5761");
        add(uri, uri, skolem);
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isTrue();
        BindingSet result = tqr.next();
        assertThat(result).matches((e) -> binds("s", uri).matches(e));
    }

    @Test
    public void testReturnFalseOnInlinedURI() throws QueryEvaluationException {
        switchToSkolem();
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnTrueOnSkolemPrefix");
        BigdataURI skolem = store().getValueFactory().createURI(scheme.value(), "91212dc3fcc8b65607d27f92b36e5761");
        add(uri, uri, skolem);
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isFalse();
    }

    @Test
    public void testReturnFalseWithoutSkolemPrefix() throws QueryEvaluationException {
        switchToSkolem();
        BigdataURI uri = store().getValueFactory().createURI("http://unittest.local/testReturnFalseWithoutSkolemPrefix");
        add(uri, uri, uri);
        add(uri, uri, store().getValueFactory().createLiteral("foo"));
        TupleQueryResult tqr = query("select ?s where { ?s <" + uri + "> ?o FILTER wikibase:isSomeValue(?o) }");
        assertThat(tqr.hasNext()).isFalse();
    }

    @Test
    public void testSomeValueModeOption() {
        assertThatThrownBy(() -> IsSomeValueFunctionFactory.SomeValueMode.lookup("test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(" [test]");

        assertThat(IsSomeValueFunctionFactory.SomeValueMode.lookup("blank"))
                .isEqualTo(IsSomeValueFunctionFactory.SomeValueMode.Blank);

        assertThat(IsSomeValueFunctionFactory.SomeValueMode.lookup("SKOLEM"))
                .isEqualTo(IsSomeValueFunctionFactory.SomeValueMode.Skolem);
    }
}
