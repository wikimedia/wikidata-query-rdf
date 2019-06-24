package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.Dct;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology.Lexeme;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary definitions for Lexemes.
 * See: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Lexemes
 */
public class LexemeVocabularyDecl extends BaseVocabularyDecl {
    public LexemeVocabularyDecl() {
        super(
            Lexeme.LEMMA, Lexeme.LEXICAL_CATEGORY, Lexeme.GRAMMATICAL_FEATURE, Dct.LANGUAGE,
            Ontolex.LEXICAL_ENTRY, Ontolex.FORM, Ontolex.SENSE, Ontolex.LEXICAL_FORM, Ontolex.SENSE_PREDICATE, Ontolex.REPRESENTATION
        );
    }
}
