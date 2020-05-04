package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.UrisScheme;

@RunWith(MockitoJUnitRunner.class)
public class BNodeSkolemizationUnitTest {
    private final ValueFactory valueFactory = new ValueFactoryImpl();
    private final URI predicate = valueFactory.createURI("http://unittest.local/predicate");
    private final URI subject = valueFactory.createURI("http://unittest.local/subject");
    private final URI object = valueFactory.createURI("http://unittest.local/object");
    private final URI context = valueFactory.createURI("http://unittest.local/context");
    private final String skolemIRIPrefix = "http://unittest.local/skolem#";

    @Mock
    UrisScheme urisScheme;

    @Test
    public void test_same_statement_without_bnode_returned() {
        Function<Statement, Statement> skolemizer = new BNodeSkolemization(valueFactory, skolemIRIPrefix);
        Statement input = valueFactory.createStatement(subject, predicate, object);
        assertThat(skolemizer.apply(input)).isSameAs(input);
    }

    @Test
    public void test_subject_skolemized() {
        Function<Statement, Statement> skolemizer = new BNodeSkolemization(valueFactory, skolemIRIPrefix);
        List<Statement> inputs = Arrays.asList(
                valueFactory.createStatement(valueFactory.createBNode("subjectbnode"), predicate, object),
                valueFactory.createStatement(valueFactory.createBNode("subjectbnode"), predicate, object, context)
        );
        for (Statement input: inputs) {
            Statement output = skolemizer.apply(input);
            assertThat(output).isNotSameAs(input);
            assertThat(output.getContext()).as("context of %s", input).isEqualTo(input.getContext());
            assertThat(output.getSubject()).as("subject of %s", input).isEqualTo(valueFactory.createURI(skolemIRIPrefix + "subjectbnode"));
            assertThat(output.getPredicate()).as("predicate of %s", input).isEqualTo(input.getPredicate());
            assertThat(output.getObject()).as("object of %s", input).isEqualTo(input.getObject());
        }
    }

    @Test
    public void test_object_skolemized() {
        Function<Statement, Statement> skolemizer = new BNodeSkolemization(valueFactory, skolemIRIPrefix);
        List<Statement> inputs = Arrays.asList(
                valueFactory.createStatement(subject, predicate, valueFactory.createBNode("objectbnode")),
                valueFactory.createStatement(subject, predicate, valueFactory.createBNode("objectbnode"), context)
        );
        for (Statement input: inputs) {
            Statement output = skolemizer.apply(input);
            assertThat(output).isNotSameAs(input);
            assertThat(output.getContext()).as("context of %s", input).isEqualTo(input.getContext());
            assertThat(output.getSubject()).as("subject of %s", input).isEqualTo(input.getSubject());
            assertThat(output.getPredicate()).as("predicate of %s", input).isEqualTo(input.getPredicate());
            assertThat(output.getObject()).as("object of %s", input).isEqualTo(valueFactory.createURI(skolemIRIPrefix + "objectbnode"));
        }
    }

    @Test
    public void test_subject_and_object_skolemized() {
        Function<Statement, Statement> skolemizer = new BNodeSkolemization(valueFactory, skolemIRIPrefix);
        List<Statement> inputs = Arrays.asList(
                valueFactory.createStatement(valueFactory.createBNode("subjectbnode"), predicate, valueFactory.createBNode("objectbnode")),
                valueFactory.createStatement(valueFactory.createBNode("subjectbnode"), predicate, valueFactory.createBNode("objectbnode"), context)
        );
        for (Statement input: inputs) {
            Statement output = skolemizer.apply(input);
            assertThat(output).isNotSameAs(input);
            assertThat(output.getContext()).as("context of %s", input).isEqualTo(input.getContext());
            assertThat(output.getSubject()).as("subject of %s", input).isEqualTo(valueFactory.createURI(skolemIRIPrefix + "subjectbnode"));
            assertThat(output.getPredicate()).as("predicate of %s", input).isEqualTo(input.getPredicate());
            assertThat(output.getObject()).as("object of %s", input).isEqualTo(valueFactory.createURI(skolemIRIPrefix + "objectbnode"));
        }
    }

    @Test
    public void test_all_bnode_skolemization() {
        when(urisScheme.property(PropertyType.DIRECT_NORMALIZED)).thenReturn("prop:wdt/");
        when(urisScheme.property(PropertyType.DIRECT)).thenReturn("prop:wdtn/");
        when(urisScheme.reference()).thenReturn("ref:");
        when(urisScheme.statement()).thenReturn("stmt:");
        when(urisScheme.wellKnownBNodeIRIPrefix()).thenReturn("http://unittest.local/.well-known/genid/");
        Collection<Statement> testData = Arrays.asList(
            statement("ref:one", "pred:is", valueFactory.createBNode("ref_one")),
            statement("entity:one", "prop:wdt/is", valueFactory.createBNode("ref_two")),
            statement("entity:one", "prop:wdtn/is", valueFactory.createBNode("ref_three")),
            statement("stmt:one", "pred:is", valueFactory.createBNode("ref_four")),
            statement("random:one", "pred:is", valueFactory.createBNode("ref_five")),
            valueFactory.createStatement(valueFactory.createBNode("ref_five"), valueFactory.createURI("pred:is"), valueFactory.createLiteral("something")),
            statement("ref:one", "pred:value", valueFactory.createLiteral("something"))
        );

        Collection<Statement> expectedData = Arrays.asList(
            statement("ref:one", "pred:is", valueFactory.createURI("http://unittest.local/.well-known/genid/ref_one")),
            statement("entity:one", "prop:wdt/is", valueFactory.createURI("http://unittest.local/.well-known/genid/ref_two")),
            statement("entity:one", "prop:wdtn/is", valueFactory.createURI("http://unittest.local/.well-known/genid/ref_three")),
            statement("stmt:one", "pred:is", valueFactory.createURI("http://unittest.local/.well-known/genid/ref_four")),
            statement("random:one", "pred:is", valueFactory.createURI("http://unittest.local/.well-known/genid/ref_five")),
            statement("http://unittest.local/.well-known/genid/ref_five", "pred:is", valueFactory.createLiteral("something")),
            statement("ref:one", "pred:value", valueFactory.createLiteral("something"))
        );

        assertThat(testData.stream().map(new BNodeSkolemization(valueFactory, urisScheme.wellKnownBNodeIRIPrefix())))
                .containsExactlyElementsOf(expectedData);
    }
}
