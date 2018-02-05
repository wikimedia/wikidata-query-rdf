package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.GeoSparql.NAMESPACE;
import static org.wikidata.query.rdf.common.uri.GeoSparql.WKT_LITERAL;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;


/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.GeoSparql} that are imported
 * into Blazegraph.
 */
public class GeoSparqlVocabularyDecl extends BaseVocabularyDecl {
    public GeoSparqlVocabularyDecl() {
        super(NAMESPACE, WKT_LITERAL);
    }
}
