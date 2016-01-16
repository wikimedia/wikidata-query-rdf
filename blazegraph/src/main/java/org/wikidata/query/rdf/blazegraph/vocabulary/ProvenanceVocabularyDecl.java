package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.Provenance.NAMESPACE;
import static org.wikidata.query.rdf.common.uri.Provenance.WAS_DERIVED_FROM;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;


/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Provenance} that are imported
 * into Blazegraph.
 */
public class ProvenanceVocabularyDecl extends BaseVocabularyDecl {
    public ProvenanceVocabularyDecl() {
        super(NAMESPACE, WAS_DERIVED_FROM);
    }
}
