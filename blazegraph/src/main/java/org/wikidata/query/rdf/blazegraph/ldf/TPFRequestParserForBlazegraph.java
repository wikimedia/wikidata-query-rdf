package org.wikidata.query.rdf.blazegraph.ldf;

import org.linkeddatafragments.fragments.tpf.TPFRequestParser;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;

/**
 * An {@link TPFRequestParser} for Blazegraph-based backends.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class TPFRequestParserForBlazegraph extends
            TPFRequestParser<BigdataValue, String, String> {

    public TPFRequestParserForBlazegraph(final LexiconRelation lexicon) {
        super(new TriplePatternElementParserForBlazegraph(lexicon));
    }
}
