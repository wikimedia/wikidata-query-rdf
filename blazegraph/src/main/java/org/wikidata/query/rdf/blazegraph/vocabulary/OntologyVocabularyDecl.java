package org.wikidata.query.rdf.blazegraph.vocabulary;

import static org.wikidata.query.rdf.common.uri.Ontology.BEST_RANK;
import static org.wikidata.query.rdf.common.uri.Ontology.DEPRECATED_RANK;
import static org.wikidata.query.rdf.common.uri.Ontology.NAMESPACE;
import static org.wikidata.query.rdf.common.uri.Ontology.NORMAL_RANK;
import static org.wikidata.query.rdf.common.uri.Ontology.PREFERRED_RANK;
import static org.wikidata.query.rdf.common.uri.Ontology.RANK;

import org.wikidata.query.rdf.common.uri.Ontology.Geo;
import org.wikidata.query.rdf.common.uri.Ontology.Quantity;
import org.wikidata.query.rdf.common.uri.Ontology.Time;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;

/**
 * Additonal vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 * @see OntologyVocabularyDecl2
 * @see OntologyVocabularyDecl3
 */
public class OntologyVocabularyDecl extends BaseVocabularyDecl {
    public OntologyVocabularyDecl() {
        super(NAMESPACE, RANK, BEST_RANK, PREFERRED_RANK, NORMAL_RANK, DEPRECATED_RANK,
                Time.VALUE, Time.PRECISION, Time.TIMEZONE, Time.CALENDAR_MODEL, Geo.LATITUDE, Geo.LONGITUDE,
                Geo.PRECISION, Geo.GLOBE, Quantity.AMOUNT, Quantity.UPPER_BOUND, Quantity.LOWER_BOUND, Quantity.UNIT);
    }
}
