package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.UrisScheme;

@RunWith(MockitoJUnitRunner.class)
public class NamespaceStatementPredicatesUnitTest {
    @Mock
    UrisScheme urisScheme;
    private NamespaceStatementPredicates predicates;
    private ValueFactory valueFactory = new ValueFactoryImpl();

    @Before
    public void init() {
        predicates = new NamespaceStatementPredicates(urisScheme);
    }

    private void isTrue(Predicate<Statement> predicate, Statement statement) {
        assertThat(predicate.test(statement)).isTrue();
    }

    private void isFalse(Predicate<Statement> predicate, Statement statement) {
        assertThat(predicate.test(statement)).isFalse();
    }

    @Test
    public void subjectInEntityDataNS() {
        when(urisScheme.entityData()).thenReturn("endata:data/");
        when(urisScheme.entityDataHttps()).thenReturn("endatas:data/");
        isTrue(predicates::subjectInEntityDataNS, statement("endata:data/something", "uri:foopred/en", "uri:bar/obj"));
        isTrue(predicates::subjectInEntityDataNS, statement("endatas:data/something", "uri:foopred/en", "uri:bar/obj"));
        isFalse(predicates::subjectInEntityDataNS, statement("uri:something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void subjectInStatementNS() {
        when(urisScheme.statement()).thenReturn("stmt:statement/");
        isTrue(predicates::subjectInStatementNS, statement("stmt:statement/something", "uri:foopred/en", "uri:bar/obj"));
        isFalse(predicates::subjectInStatementNS, statement("uri:something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void subjectInReferenceNS() {
        when(urisScheme.reference()).thenReturn("ref:reference/");
        isTrue(predicates::subjectInReferenceNS, statement("ref:reference/something", "uri:foopred/en", "uri:bar/obj"));
        isFalse(predicates::subjectInReferenceNS, statement("uri:something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void objectInReferenceNS() {
        when(urisScheme.reference()).thenReturn("ref:reference/");
        isTrue(predicates::objectInReferenceNS, statement("uri:something", "uri:foopred/en", "ref:reference/something"));
        isFalse(predicates::objectInReferenceNS, statement("uri:something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void subjectInValueNS() {
        when(urisScheme.value()).thenReturn("val:value/");
        isTrue(predicates::subjectInValueNS, statement("val:value/something", "uri:foopred/en", "uri:bar/obj"));
        isFalse(predicates::subjectInValueNS, statement("uri:/something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void objectInValueNS() {
        when(urisScheme.value()).thenReturn("val:value/");
        isTrue(predicates::objectInValueNS, statement("uri:something", "uri:foopred/en", "val:value/something"));
        isFalse(predicates::objectInValueNS, statement("uri:something", "uri:foopred/en", "uri:bar/obj"));
    }

    @Test
    public void tripleRefValue() {
        when(urisScheme.property(PropertyType.REFERENCE_VALUE)).thenReturn("prop:refval/");
        when(urisScheme.property(PropertyType.REFERENCE_VALUE_NORMALIZED)).thenReturn("prop:refvalnorm/");
        when(urisScheme.value()).thenReturn("val:value/");
        when(urisScheme.reference()).thenReturn("ref:reference/");

        Predicate<Statement> predicate = predicates::tripleRefValue;
        isTrue(predicate, statement("ref:reference/ref-id", "prop:refval/P12", "val:value/val-id"));
        isTrue(predicate, statement("ref:reference/ref-id", "prop:refvalnorm/P12", "val:value/val-id"));
        isFalse(predicate, statement("usr:foo", "prop:refval/P12", "val:value/val-id"));
        isFalse(predicate, statement("ref:reference/ref-id", "uri:foo", "val:value/val-id"));
        isFalse(predicate, statement("ref:reference/ref-id", "prop:refval/P12", "uri:foo"));

    }

    @Test
    public void reificationStatement() {
        when(urisScheme.statement()).thenReturn("stmt:statement/");
        when(urisScheme.property(PropertyType.CLAIM)).thenReturn("prop:claim/");
        Predicate<Statement> predicate = predicates::reificationStatement;
        isTrue(predicate, statement("uri:entity/123", "prop:claim/P12", "stmt:statement/ST-ID"));
        isTrue(predicate, statement("uri:foo", "prop:claim/P12", "stmt:statement/ST-ID"));
        isFalse(predicate, statement("uri:entity/123", "prop:claim/P12", "uri:foo"));
        isFalse(predicate, statement("uri:entity/123", "uri:foo", "stmt:statement/ST-ID"));
    }

    @Test
    public void bnodeOrSkolemIRI() {
        when(urisScheme.wellKnownBNodeIRIPrefix()).thenReturn("main:.well-known/");
        Predicate<Statement> predicate = predicates::subjectIsBNodeOrSkolemIRI;
        isTrue(predicate, statement("main:.well-known/123", "prop:claim/P12", "stmt:statement/ST-ID"));
        isTrue(predicate, valueFactory.createStatement(valueFactory.createBNode("123"),
                valueFactory.createURI("prop:claim/P12"), valueFactory.createURI("stmt:statement/ST-ID")));
        isFalse(predicate, statement("main:/pref/123", "prop:claim/P12", "stmt:statement/ST-ID"));
    }
}
