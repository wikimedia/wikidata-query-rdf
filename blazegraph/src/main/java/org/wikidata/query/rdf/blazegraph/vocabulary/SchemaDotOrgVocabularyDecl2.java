package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.IS_PART_OF;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.NAME;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Additional vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class SchemaDotOrgVocabularyDecl2 extends BaseVocabularyDecl {
    public SchemaDotOrgVocabularyDecl2() {
        super(NAME, IS_PART_OF);
    }
}
