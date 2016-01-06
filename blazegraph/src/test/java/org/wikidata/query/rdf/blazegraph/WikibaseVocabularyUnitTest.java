package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.instanceOf;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.uri.VocabURIByteIV;
import com.bigdata.rdf.model.BigdataStatement;

public class WikibaseVocabularyUnitTest extends AbstractRandomizedBlazegraphTestBase {
    @Test
    public void ranksAreBytes() {
        BigdataStatement statement = roundTrip("s:Q23-uuidhere", Ontology.RANK, Ontology.BEST_RANK);
        assertThat(statement.getSubject().getIV(), instanceOf(TermId.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void labelIsByte() {
        BigdataStatement statement = roundTrip("entity:Q23", RDFS.LABEL, new LiteralImpl("George", "en"));
        /*
         * See WikibaseInlineUrIFactoryUnitTest for tests about the entity
         * namespace.
         */
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void altLabelIsByte() {
        BigdataStatement statement = roundTrip("entity:Q23", SKOS.ALT_LABEL, new LiteralImpl("G", "en"));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void descriptionIsByte() {
        BigdataStatement statement = roundTrip("entity:Q23", SchemaDotOrg.DESCRIPTION, new LiteralImpl("some dude",
                "en"));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void dateModifiedIsByte() {
        BigdataStatement statement = roundTrip("entity:Q23", SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("its a date!",
                "en"));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void versionIsByte() {
        BigdataStatement statement = roundTrip("entity:Q23", SchemaDotOrg.VERSION,
                new LiteralImpl("its a number", "en"));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void entityTruthyAndStatementAreBytes() {
        BigdataStatement statement = roundTrip(uris().entity(), uris().property(PropertyType.DIRECT) + "P", uris().statement());
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void valueReferenceAndQualifierAreBytes() {
        BigdataStatement statement = roundTrip(uris().value(), uris().property(PropertyType.REFERENCE) + "P", uris().property(PropertyType.QUALIFIER) + "P");
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

    /**
     * Make sure the "P" and "Q" suffixed versions of entity are a byte because
     * if they aren't then inline uris are more bloated.
     */
    @Test
    public void entityPAndQAreBytes() {
        BigdataStatement statement = roundTrip(uris().entity() + "P", uris().entity() + "Q", new LiteralImpl("cat"));
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void truthyPAndQAreBytes() {
        BigdataStatement statement = roundTrip(uris().entity() + "Q", uris().property(PropertyType.DIRECT) + "P", new LiteralImpl("cat"));
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void valuePAndQAreBytes() {
        BigdataStatement statement = roundTrip(uris().entity() + "P", uris().property(PropertyType.STATEMENT_VALUE) + "P", new LiteralImpl("cat"));
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void qualifierPAndQAreBytes() {
        BigdataStatement statement = roundTrip(uris().entity() + "P", uris().property(PropertyType.QUALIFIER) + "P", new LiteralImpl(
                "cat"));
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void quantityIsInlined() {
        BigdataStatement statement = roundTrip(Ontology.Quantity.AMOUNT, Ontology.Quantity.UNIT,
                Ontology.Quantity.UPPER_BOUND);
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void timeIsInlined() {
        BigdataStatement statement = roundTrip(Ontology.Time.VALUE, Ontology.Time.PRECISION,
                Ontology.Time.CALENDAR_MODEL);
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

    @Test
    public void geoIsInlined() {
        BigdataStatement statement = roundTrip(Ontology.Geo.LATITUDE, Ontology.Geo.LONGITUDE, Ontology.Geo.GLOBE);
        assertThat(statement.getSubject().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getPredicate().getIV(), instanceOf(VocabURIByteIV.class));
        assertThat(statement.getObject().getIV(), instanceOf(VocabURIByteIV.class));
    }

}
