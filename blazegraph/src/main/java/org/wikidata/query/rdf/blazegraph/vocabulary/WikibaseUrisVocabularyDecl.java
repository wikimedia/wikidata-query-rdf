package org.wikidata.query.rdf.blazegraph.vocabulary;

import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class WikibaseUrisVocabularyDecl extends BaseVocabularyDecl {
    public WikibaseUrisVocabularyDecl(WikibaseUris uris) {
        super(uris.entity(), uris.truthy(), uris.statement(), uris.reference(), uris.qualifier(),//
                /*
                 * Note that these next two lines are required to make
                 * WikibaseInlineUriFactory work with
                 * IntegerSuffixInlineUriHandler which is required so we can
                 * store entities as unsigned integers.
                 */
                uris.entity() + "P", uris.truthy() + "P", uris.value() + "P", uris.qualifier() + "P",//
                uris.entity() + "Q", uris.truthy() + "Q", uris.value() + "Q", uris.qualifier() + "Q",//
                uris.value() // Used by GUID identified values
        );
    }
}
