package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.CommonValues;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary declaration for common uris that aren't actually part of wikibase
 * but are common values in Wikidata.
 */
public class CommonValuesVocabularyDecl extends BaseVocabularyDecl {
    public CommonValuesVocabularyDecl() {
        super(CommonValues.VIAF);
    }
}
