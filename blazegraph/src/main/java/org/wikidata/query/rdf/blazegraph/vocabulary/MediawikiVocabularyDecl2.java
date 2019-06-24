package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.Mediawiki;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Mediawiki} that are imported
 * into Blazegraph.
 */
public class MediawikiVocabularyDecl2 extends BaseVocabularyDecl {
    public MediawikiVocabularyDecl2() {
        super(Mediawiki.HIDDEN_CATEGORY, Mediawiki.PAGES, Mediawiki.SUBCATEGORIES);
    }
}
