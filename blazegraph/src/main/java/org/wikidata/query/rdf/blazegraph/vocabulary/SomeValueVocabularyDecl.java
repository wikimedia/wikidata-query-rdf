package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.UrisScheme;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Needed for somevalue inline and perf improvements for wikibase:isSomeValue().
 */
public class SomeValueVocabularyDecl extends BaseVocabularyDecl {
    public SomeValueVocabularyDecl(UrisScheme uriSystem) {
        super(uriSystem.wellKnownBNodeIRIPrefix());
    }
}
