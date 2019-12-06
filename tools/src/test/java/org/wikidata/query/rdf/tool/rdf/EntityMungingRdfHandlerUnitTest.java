package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.wikidata.query.rdf.test.StatementHelper.siteLink;
import static org.wikidata.query.rdf.test.StatementHelper.statement;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.test.Randomizer;

@RunWith(MockitoJUnitRunner.class)
public class EntityMungingRdfHandlerUnitTest {
    @Captor
    private ArgumentCaptor<Statement> statements;

    @Rule
    public final Randomizer randomizer = new Randomizer();

    @Test
    public void testSmallDump() throws RDFParseException, IOException, RDFHandlerException {
        UrisScheme uriScheme = UrisSchemeFactory.WIKIDATA;
        Munger munger = Munger.builder(uriScheme).build();
        RDFHandler childHandler = mock(RDFHandler.class);
        AtomicLong nbEntities = new AtomicLong();
        EntityMungingRdfHandler handler = new EntityMungingRdfHandler(uriScheme, munger, childHandler, nbEntities::set);
        RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
        parser.setRDFHandler(handler);
        parser.parse(this.getClass().getResourceAsStream("small_dump.ttl"), uriScheme.root());
        verify(childHandler, atLeastOnce()).handleStatement(statements.capture());
        assertThat(munger.getDumpFormatVersion()).isEqualTo("1.0.0");
        assertThat(statements.getAllValues()).contains(
                // initial Dump statements are kept and passsed through
                statement("http://wikiba.se/ontology#Dump", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://schema.org/Dataset"),
                // a random statement
                statement("https://en.wikipedia.org/wiki/Resource_Description_Framework", "http://schema.org/about", "http://www.wikidata.org/entity/Q54872"),
                statement("http://www.wikidata.org/entity/Q6825", "http://www.w3.org/2002/07/owl#sameAs", "http://www.wikidata.org/entity/Q26709563"),
                statement("http://www.wikidata.org/entity/Q6825", "http://schema.org/version", 942411738), // redirect revision ID
                statement("https://it.wikivoyage.org/wiki/Belgio", "http://schema.org/about", "http://www.wikidata.org/entity/Q31")
        );
        // Check that the blank node is kept
        assertThat(statements.getAllValues())
                .filteredOn(s -> s.getSubject().stringValue().equals("http://www.wikidata.org/entity/Q54872") &&
                        s.getPredicate().stringValue().equals("http://www.wikidata.org/prop/direct/P576"))
                .size()
                .isEqualTo(1);

        assertThat(nbEntities.intValue()).isEqualTo(3);
    }

    @Test
    public void testEntityBoundary() throws RDFHandlerException {
        UrisScheme uriScheme = UrisSchemeFactory.WIKIDATA;
        Munger munger = Munger.builder(uriScheme).build();
        RDFHandler childHandler = mock(RDFHandler.class);
        AtomicLong nbEntities = new AtomicLong();
        EntityMungingRdfHandler handler = new EntityMungingRdfHandler(uriScheme, munger, childHandler, nbEntities::set);

        handler.startRDF();
        handler.handleStatement(statement(uriScheme.entityDataHttps() + "Q1", SchemaDotOrg.ABOUT, uriScheme.entityIdToURI("Q1")));
        handler.handleStatement(statement(uriScheme.entityDataHttps() + "Q1", SchemaDotOrg.VERSION, 123));
        handler.handleStatement(statement(uriScheme.entityDataHttps() + "Q1", SchemaDotOrg.SOFTWARE_VERSION, 2));
        handler.handleStatement(statement(uriScheme.entityDataHttps() + "Q1", SchemaDotOrg.DATE_MODIFIED, new LiteralImpl("2019-11-19T15:53:28Z")));
        for (Statement statement : siteLink("Q1", "https://en.wikipedia.org/wiki/Thing", "en")) {
            handler.handleStatement(statement);
        }
        // It buffers everything until it reaches another entoty
        assertThat(nbEntities.get()).isEqualTo(0);
        verify(childHandler).startRDF();
        verify(childHandler, never()).handleStatement(any());
        // Send another data:entity statement so that we flush the buffer to the munger
        handler.handleStatement(statement(uriScheme.entityDataHttps() + "Q2", SchemaDotOrg.ABOUT, uriScheme.entityIdToURI("Q2")));
        assertThat(nbEntities.get()).isEqualTo(1);
    }

    //TODO: test redirects boundary
}
