package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

public class StatementPredicatesUnitTest {

    @Test
    public void softwareVersion() {
        assertThat(StatementPredicates.softwareVersion(statement("uri:foo", SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.softwareVersion(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void dumpStatement() {
        assertThat(StatementPredicates.dumpStatement(statement(Ontology.DUMP, "uri:bar", new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.dumpStatement(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void dumpFormatVersion() {
        assertThat(StatementPredicates.dumpFormatVersion(statement(Ontology.DUMP, SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.dumpFormatVersion(statement(Ontology.DUMP, "uri:bar", new LiteralImpl("bar")))).isFalse();
        assertThat(StatementPredicates.dumpFormatVersion(statement("uri:foo", SchemaDotOrg.SOFTWARE_VERSION, new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void redirect() {
        assertThat(StatementPredicates.redirect(statement("uri:foo", OWL.SAME_AS, new LiteralImpl("bar")))).isTrue();
        assertThat(StatementPredicates.redirect(statement("uri:foo", "uri:bar", new LiteralImpl("bar")))).isFalse();
    }

    @Test
    public void typeStatement() {
        assertThat(StatementPredicates.typeStatement(statement("uri:foo", RDF.TYPE, "uri:footype"))).isTrue();
        assertThat(StatementPredicates.typeStatement(statement("uri:foo", "uri:bartype", "uri:footype"))).isFalse();
    }

    @Test
    public void referenceTypeStatement() {
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", RDF.TYPE, Ontology.REFERENCE))).isTrue();
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", RDF.TYPE, "uri:foo"))).isFalse();
        assertThat(StatementPredicates.referenceTypeStatement(statement("ref:XYZ", "uri:foo", Ontology.REFERENCE))).isFalse();
    }

    @Test
    public void valueTypeStatement() {
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Time.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Geo.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, Ontology.Quantity.TYPE))).isTrue();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", RDF.TYPE, "uri:foo"))).isFalse();
        assertThat(StatementPredicates.valueTypeStatement(statement("val:XYZ", "uri:foo", Ontology.Time.TYPE))).isFalse();
    }

    @Test
    public void wikiGroups() {
        assertThat(StatementPredicates.wikiGroupDefinition(statement("val:XYZ", Ontology.WIKIGROUP, "uri:foo"))).isTrue();
        assertThat(StatementPredicates.wikiGroupDefinition(statement("val:XYZ", RDF.TYPE, "uri:foo"))).isFalse();
    }
}
