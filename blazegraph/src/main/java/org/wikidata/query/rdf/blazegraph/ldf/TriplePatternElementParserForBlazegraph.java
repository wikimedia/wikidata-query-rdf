package org.wikidata.query.rdf.blazegraph.ldf;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

import org.linkeddatafragments.util.TriplePatternElementParser;

/**
 * A {@link TriplePatternElementParser} for Blazegraph-based backends.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class TriplePatternElementParserForBlazegraph
        extends
            TriplePatternElementParser<BigdataValue, String, String> {
    /**
     * Blazegraph value factory.
     */
    private final BigdataValueFactory valueFactory;

    public TriplePatternElementParserForBlazegraph(final LexiconRelation lex) {
        valueFactory = lex.getValueFactory();
    }

    @Override
    public String createNamedVariable(final String varName) {
        return varName;
    }

    @Override
    public String createAnonymousVariable(final String label) {
        return label;
    }

    @Override
    public BigdataValue createBlankNode(final String label) {
        return valueFactory.createBNode(label);
    }

    @Override
    public BigdataValue createURI(final String uri) {
        return valueFactory.createURI(uri);
    }

    @Override
    public BigdataValue createTypedLiteral(final String label,
            final String typeURI) {
        final BigdataURI datatypeURI = valueFactory.createURI(typeURI);
        return valueFactory.createLiteral(label, datatypeURI);
    }

    @Override
    public BigdataValue createLanguageLiteral(final String label,
            final String languageTag) {
        return valueFactory.createLiteral(label, languageTag);
    }

    @Override
    public BigdataValue createPlainLiteral(final String label) {
        return valueFactory.createLiteral(label);
    }

    @Override
    public BigdataValue handleUnparsableParameter(final String parameter) {
        throw new IllegalArgumentException();
    }
}
