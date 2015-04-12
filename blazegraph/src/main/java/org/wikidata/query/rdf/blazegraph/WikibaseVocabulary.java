package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.vocabulary.CommonValuesVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.OntologyVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.ProvenanceVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.SchemaDotOrgVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.WikibaseUrisVocabularyDecl;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.rdf.vocab.DefaultBigdataVocabulary;

/**
 * Versioned vocabulary classes for wikibase. All classes need a namespace and a
 * default constructor or Blazegraph blows up on them.
 */
public class WikibaseVocabulary {
    public static class V001 extends DefaultBigdataVocabulary {
        public V001() {
        }

        public V001(String namespace) {
            super(namespace);
        }

        @Override
        protected void addValues() {
            // TODO lookup wikibase host and default to wikidata
            addDecl(new WikibaseUrisVocabularyDecl(WikibaseUris.WIKIDATA));
            addDecl(new OntologyVocabularyDecl());
            addDecl(new SchemaDotOrgVocabularyDecl());
            addDecl(new ProvenanceVocabularyDecl());
            addDecl(new CommonValuesVocabularyDecl());
            super.addValues();
        }
    }
}
