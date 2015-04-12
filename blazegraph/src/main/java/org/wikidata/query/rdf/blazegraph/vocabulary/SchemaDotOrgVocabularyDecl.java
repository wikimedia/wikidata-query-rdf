package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.ABOUT;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.ARTICLE;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.DATE_MODIFIED;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.DESCRIPTION;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.IN_LANGUAGE;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.NAMESPACE;
import static org.wikidata.query.rdf.common.uri.SchemaDotOrg.VERSION;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class SchemaDotOrgVocabularyDecl extends BaseVocabularyDecl {
    public SchemaDotOrgVocabularyDecl() {
        super(NAMESPACE, VERSION, DATE_MODIFIED, ABOUT, ARTICLE, IN_LANGUAGE, DESCRIPTION);
    }
}
