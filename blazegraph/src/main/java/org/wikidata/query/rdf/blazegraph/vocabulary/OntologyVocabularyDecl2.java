package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.Ontology.BADGE;
import static org.wikidata.query.rdf.common.uri.Ontology.WIKIGROUP;
import static org.wikidata.query.rdf.common.uri.Ontology.STATEMENTS;
import static org.wikidata.query.rdf.common.uri.Ontology.SITELINKS;

import org.wikidata.query.rdf.common.uri.Ontology.Geo;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.Ontology.Time;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class OntologyVocabularyDecl2 extends BaseVocabularyDecl {
    public OntologyVocabularyDecl2() {
        super(Quantity.NORMALIZED, Quantity.TYPE, Time.TYPE, Geo.TYPE, BADGE,
                WIKIGROUP, STATEMENTS, SITELINKS);
    }
}
