package org.wikidata.query.rdf.tool.rdf;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikidata.query.rdf.test.StatementHelper.fullSiteLink;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.turtle.TurtleWriter;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

public class SiteLinksReclassificationUnitTest {
    private final Munger munger = Munger.builder(UrisSchemeFactory.WIKIDATA).build();
    private final EntityDiff diff = EntityDiff.withWikibaseSharedElements(UrisSchemeFactory.WIKIDATA);

    @Test
    public void test_add_rename_and_delete() {
        List<Statement> siteLink1Typo = new ArrayList<>(fullSiteLink("Q1", "http://en.mysitelink.test/Q1_typo", "https://en.mysitelink.test/", "en"));
        Statement q1toLink1Typo = statement("http://en.mysitelink.test/Q1_typo", SchemaDotOrg.ABOUT, "Q1");
        List<Statement> siteLink1Renamed = new ArrayList<>(fullSiteLink("Q1", "http://en.mysitelink.test/Q1", "https://en.mysitelink.test/", "en"));
        Statement q1toLink1Renamed = statement("http://en.mysitelink.test/Q1", SchemaDotOrg.ABOUT, "Q1");
        List<Statement> siteLink2Added = new ArrayList<>(fullSiteLink("Q1", "http://de.mysitelink.test/Q1", "https://de.mysitelink.test/", "de"));
        Statement q1toLink2Added = statement("http://de.mysitelink.test/Q1", SchemaDotOrg.ABOUT, "Q1");
        List<Statement> siteLink3Removed = new ArrayList<>(fullSiteLink("Q1", "http://ce.mysitelink.test/Q1", "https://ce.mysitelink.test/", "ce"));
        Statement q1toLink3Removed = statement("http://ce.mysitelink.test/Q1", SchemaDotOrg.ABOUT, "Q1");


        List<Statement> added = new ArrayList<>(siteLink1Renamed);
        added.addAll(siteLink2Added);
        added.add(statement("uri:added-1", "uri:added-1", "uri:added-1"));

        List<Statement> removed = new ArrayList<>(siteLink3Removed);
        removed.addAll(siteLink1Typo);
        removed.add(statement("uri:removed-1", "uri:removed-1", "uri:removed-1"));

        List<Statement> linkedShared = singletonList(statement("uri:linked-1", "uri:linked-1", "uri:linked-1"));
        List<Statement> unlinkedShared = singletonList(statement("uri:unlinked-1", "uri:unlinked-1", "uri:unlinked-1"));

        Patch patch = new Patch(added, linkedShared, removed, unlinkedShared);

        List<Statement> expectedAdded = new ArrayList<>();
        expectedAdded.add(statement("uri:added-1", "uri:added-1", "uri:added-1"));
        expectedAdded.add(q1toLink2Added);
        expectedAdded.add(q1toLink1Renamed);

        List<Statement> expectedRemoved = new ArrayList<>();
        expectedRemoved.add(q1toLink1Typo);
        expectedRemoved.add(statement("uri:removed-1", "uri:removed-1", "uri:removed-1"));
        expectedRemoved.add(q1toLink3Removed);

        List<Statement> expectedLinkedShared = new ArrayList<>();
        expectedLinkedShared.add(statement("uri:linked-1", "uri:linked-1", "uri:linked-1"));
        expectedLinkedShared.addAll(siteLink1Renamed);
        expectedLinkedShared.addAll(siteLink2Added);
        assertThat(expectedLinkedShared.remove(q1toLink1Renamed)).isTrue();
        assertThat(expectedLinkedShared.remove(q1toLink2Added)).isTrue();

        List<Statement> expectedUnlinkedShared = new ArrayList<>(siteLink3Removed);
        expectedUnlinkedShared.addAll(siteLink1Typo);
        assertThat(expectedUnlinkedShared.remove(q1toLink3Removed)).isTrue();
        assertThat(expectedUnlinkedShared.remove(q1toLink1Typo)).isTrue();
        expectedUnlinkedShared.add(statement("uri:unlinked-1", "uri:unlinked-1", "uri:unlinked-1"));

        Patch actualPatch = SiteLinksReclassification.reclassify(patch);

        assertThat(actualPatch.getAdded()).containsExactlyInAnyOrderElementsOf(expectedAdded);
        assertThat(actualPatch.getRemoved()).containsExactlyInAnyOrderElementsOf(expectedRemoved);
        assertThat(actualPatch.getLinkedSharedElements()).containsExactlyInAnyOrderElementsOf(expectedLinkedShared);
        assertThat(actualPatch.getUnlinkedSharedElements()).containsExactlyInAnyOrderElementsOf(expectedUnlinkedShared);
    }

    @Test
    public void test_with_wikidata_entities() throws IOException, RDFHandlerException, RDFParseException {
        Collection<Statement> rev1 = load("sitelink_class_rev_1.ttl", (s) -> munger.munge("Q2", s));
        Collection<Statement> rev2 = load("sitelink_class_rev_2.ttl", (s) -> munger.munge("Q2", s));
        Patch patch = diff.diff(rev1, rev2);
        patch = SiteLinksReclassification.reclassify(patch);
        assertThat(patch.getAdded()).containsExactlyInAnyOrderElementsOf(load("sitelink_class_diff_rev_1_2_added.ttl", (s) -> {}));
        assertThat(patch.getRemoved()).containsExactlyInAnyOrderElementsOf(load("sitelink_class_diff_rev_1_2_removed.ttl", (s) -> {}));
        assertThat(patch.getLinkedSharedElements()).containsExactlyInAnyOrderElementsOf(load("sitelink_class_diff_rev_1_2_linkedshared.ttl", (s) -> {}));
        assertThat(patch.getUnlinkedSharedElements()).containsExactlyInAnyOrderElementsOf(load("sitelink_class_diff_rev_1_2_unlinkedshared.ttl", (s) -> {}));
    }

    @Test
    public void test_inconsistent_sitelink_without_ispartof_triple() {
        String siteLink = "http://en.sitelink.unittest.local/Entity";
        String site = "http://en.sitelink.unittest.local/";
        Statement siteLinkBlock = statement(siteLink, SchemaDotOrg.IS_PART_OF, site);
        List<Statement> stmts = new ArrayList<>(fullSiteLink("Q1", siteLink, site, "en"));
        assertThat(stmts.remove(siteLinkBlock)).isTrue();
        final Patch addPatch = new Patch(stmts, emptyList(), emptyList(), emptyList());
        assertThatThrownBy(() -> SiteLinksReclassification.reclassify(addPatch),
                "trying to add a site link block without a schema:ispartof statement must be raise an exception")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot find schema:isPartOf for sitelink " + siteLink);

        Patch delPatch = new Patch(emptyList(), emptyList(), stmts, emptyList());
        assertThatThrownBy(() -> SiteLinksReclassification.reclassify(delPatch),
                "trying to delete a site link block without a schema:ispartof statement must be raise an exception")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot find schema:isPartOf for sitelink " + siteLink);
    }

    @Test
    public void test_duplicated_sitelink_targets() {
        String siteLink1 = "http://en.sitelink.unittest.local/Entity1";
        String siteLink2 = "http://en.sitelink.unittest.local/Entity2";
        String site = "http://en.sitelink.unittest.local/";
        List<Statement> stmts = new ArrayList<>(fullSiteLink("Q1", siteLink1, site, "en"));
        stmts.addAll(fullSiteLink("Q1", siteLink2, site, "en"));
        final Patch addPatch = new Patch(stmts, emptyList(), emptyList(), emptyList());
        assertThatThrownBy(() -> SiteLinksReclassification.reclassify(addPatch),
                "trying to add multiple site link to the same site must be raise an exception")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Same site [" + site + "] for ", siteLink1, siteLink2);

        Patch delPatch = new Patch(emptyList(), emptyList(), stmts, emptyList());
        assertThatThrownBy(() -> SiteLinksReclassification.reclassify(delPatch),
                "trying to delete multiple site link to the same site must be raise an exception")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll("Same site [" + site + "] for ", siteLink1, siteLink2);
    }

    /**
     * small utility method kept around to write the tested fixtures if something has to change.
     */
    private void write(Collection<Statement> stmts, String res) throws RDFHandlerException, IOException {
        try (Writer w = Files.newBufferedWriter(Paths.get(res), StandardCharsets.UTF_8)) {
            RDFWriter rdfwriter = new TurtleWriter(w);
            rdfwriter.startRDF();
            rdfwriter.handleNamespace("wd", UrisSchemeFactory.WIKIDATA.entityURIs().stream().findFirst().get());
            rdfwriter.handleNamespace("schema", SchemaDotOrg.NAMESPACE);
            rdfwriter.handleNamespace("wikibase", Ontology.NAMESPACE);
            for (Statement s : stmts.stream().sorted(Comparator.comparing(s -> s.getSubject().stringValue())).collect(Collectors.toList())) {
                rdfwriter.handleStatement(s);
            }
            rdfwriter.endRDF();
        }
    }

    private Collection<Statement> load(String s, Consumer<Collection<Statement>> munge) throws IOException, RDFParseException, RDFHandlerException {
        StatementCollector collector = new StatementCollector();
        try (InputStream is = this.getClass().getResourceAsStream(s)) {
            RDFParserSuppliers.defaultRdfParser().get(collector).parse(is, "unused");
            munge.accept(collector.getStatements());
            return collector.getStatements();
        }
    }
}
