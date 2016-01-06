package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.vocabulary.CommonValuesVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.OntologyVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.ProvenanceVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.SchemaDotOrgVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.WikibaseUrisVocabularyDecl;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.rdf.vocab.DefaultBigdataVocabulary;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;

/**
 * Versioned vocabulary classes for wikibase. All classes need a namespace and a
 * default constructor or Blazegraph blows up on them.
 */
public class WikibaseVocabulary {

    /**
     * Current vocabulary class.
     */
    public static final Class VOCABULARY_CLASS = V001.class;

    protected WikibaseVocabulary() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }

    /**
     * Vocabulary classes
     * V001: 1.5.x version
     * V002: 2.0 version, extends different class
     */

    /**
     * Vocabulary class for BG 1.5.
     * Inherits different vocabulary
     */
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

    /**
     * Class for BG 2.0.
     * Inherits different vocabulary
     */
    public static class V002 extends BigdataCoreVocabulary_v20151210 {
        public V002() {
        }

        public V002(String namespace) {
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
