package org.wikidata.query.rdf.blazegraph;

import java.util.List;

import org.wikidata.query.rdf.blazegraph.vocabulary.CommonValuesVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.CommonValuesVocabularyDecl2;
import org.wikidata.query.rdf.blazegraph.vocabulary.CommonValuesVocabularyDecl3;
import org.wikidata.query.rdf.blazegraph.vocabulary.GeoSparqlVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.LexemeVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.MediawikiVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.MediawikiVocabularyDecl2;
import org.wikidata.query.rdf.blazegraph.vocabulary.OntologyVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.OntologyVocabularyDecl2;
import org.wikidata.query.rdf.blazegraph.vocabulary.OntologyVocabularyDecl3;
import org.wikidata.query.rdf.blazegraph.vocabulary.ProvenanceVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.SchemaDotOrgVocabularyDecl;
import org.wikidata.query.rdf.blazegraph.vocabulary.SchemaDotOrgVocabularyDecl2;
import org.wikidata.query.rdf.blazegraph.vocabulary.WikibaseUrisVocabularyDecl;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.common.uri.PropertyType;

import com.bigdata.rdf.vocab.DefaultBigdataVocabulary;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20160317;

/**
 * Versioned vocabulary classes for wikibase. All classes need a namespace and a
 * default constructor or Blazegraph blows up on them.
 */
public class WikibaseVocabulary {

    /**
     * Current vocabulary class, for tests.
     */
    public static final Class VOCABULARY_CLASS = V004.class;

    protected WikibaseVocabulary() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }

    /*
     * Vocabulary classes
     * V001: 1.5.x version
     * V002: 2.0 version, extends different class
     * V003: V002 with new normalized predicates and Mediawiki ones added
     * V004: V003 plus new predicates & Lexeme support
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
            addDecl(new WikibaseUrisVocabularyDecl(UrisSchemeFactory.getURISystem(), getSuffixes()));
            addDecl(new OntologyVocabularyDecl());
            addDecl(new SchemaDotOrgVocabularyDecl2());
            addDecl(new ProvenanceVocabularyDecl());
            addDecl(new CommonValuesVocabularyDecl());
            super.addValues();
        }

        /**
         * Get list of suffixes to use for property predicates.
         */
        protected List<String> getSuffixes() {
            return PropertyType.suffixes(PropertyType.V001());
        }
    }

    /**
     * Class for BG 2.0 with geospatial.
     * Inherits different vocabulary and adds geospatial types.
     */
    public static class V002 extends BigdataCoreVocabulary_v20160317 {

        @SuppressWarnings("unused")
        private static final long serialVersionUID = -1324123255255667253L;

        public V002() {
        }

        public V002(String namespace) {
            super(namespace);
        }

        @Override
        protected void addValues() {
            // TODO lookup wikibase host and default to wikidata
            addDecl(new WikibaseUrisVocabularyDecl(UrisSchemeFactory.getURISystem(), getSuffixes()));
            addDecl(new OntologyVocabularyDecl());
            addDecl(new SchemaDotOrgVocabularyDecl());
            addDecl(new ProvenanceVocabularyDecl());
            addDecl(new CommonValuesVocabularyDecl());
            addDecl(new GeoSparqlVocabularyDecl());
            super.addValues();
        }

        /**
         * Get list of suffixes to use for property predicates.
         */
        protected List<String> getSuffixes() {
            return PropertyType.suffixes(PropertyType.V001());
        }
    }

    /**
     * Class with extended vocabulary for new Wikidata predicates.
     */
    public static class V003 extends V002 {
        public V003() {
        }

        public V003(String namespace) {
            super(namespace);
        }

        @Override
        protected List<String> getSuffixes() {
            return PropertyType.suffixes();
        }

        @Override
        protected void addValues() {
            super.addValues();
            addDecl(new OntologyVocabularyDecl2());
            addDecl(new SchemaDotOrgVocabularyDecl2());
            addDecl(new CommonValuesVocabularyDecl2());
            addDecl(new MediawikiVocabularyDecl());
        }
    }

    public static class V004 extends V003 {
        public V004() {
        }

        public V004(String namespace) {
            super(namespace);
        }

        @Override
        protected void addValues() {
            super.addValues();
            addDecl(new CommonValuesVocabularyDecl3());
            addDecl(new OntologyVocabularyDecl3());
            addDecl(new MediawikiVocabularyDecl2());
            addDecl(new LexemeVocabularyDecl());
        }
    }
}
