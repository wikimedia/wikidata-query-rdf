package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class OntologyVocabularyDecl3 extends BaseVocabularyDecl {
    public OntologyVocabularyDecl3() {
        super(Ontology.IDENTIFIERS, Ontology.TIMESTAMP, Ontology.CONSTRAINT_VIOLATION);
    }
}
