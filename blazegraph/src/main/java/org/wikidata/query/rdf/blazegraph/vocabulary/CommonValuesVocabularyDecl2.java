package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.CommonValues;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vabulary declaration for common uris that aren't actually part of wikibase
 * but are common values in Wikidata.
 * @see CommonValuesVocabularyDecl2
 * @see CommonValuesVocabularyDecl3
 */
public class CommonValuesVocabularyDecl2 extends BaseVocabularyDecl {
    public CommonValuesVocabularyDecl2() {
        super(CommonValues.GEONAMES, CommonValues.PUBCHEM, CommonValues.CHEMSPIDER);
    }
}
