package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.Mediawiki.CATEGORY;
import static org.wikidata.query.rdf.common.uri.Mediawiki.IS_IN_CATEGORY;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Mediawiki} that are imported
 * into Blazegraph.
 */
public class MediawikiVocabularyDecl extends BaseVocabularyDecl {
    public MediawikiVocabularyDecl() {
        super(CATEGORY, IS_IN_CATEGORY);
    }
}
