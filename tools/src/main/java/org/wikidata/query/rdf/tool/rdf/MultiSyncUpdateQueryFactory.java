package org.wikidata.query.rdf.tool.rdf;

import static org.wikidata.query.rdf.tool.Utils.loadBody;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;

final class MultiSyncUpdateQueryFactory {

    private final UrisScheme uris;
    /**
     * SPARQL for a portion of the update, batched sync - pre-filled with static elements of the query.
     */
    private final String msyncBody;
    /**
     * SPARQL for a portion of the update.
     */
    private final String cleanUnused;

    MultiSyncUpdateQueryFactory(UrisScheme uris) {
        this.uris = uris;

        cleanUnused = loadBody("CleanUnused", MultiSyncUpdateQueryFactory.class);

        msyncBody = getPrefilledTemplate(uris);
    }

    String buildQuery(Set<String> entityIds,
                      List<Statement> insertStatements,
                      ClassifiedStatements classifiedStatements,
                      Set<String> valueSet,
                      Set<String> refSet,
                      List<String> lexemeSubIds,
                      Instant timestamp) {

        UpdateBuilder updateBuilder = new UpdateBuilder(msyncBody);
        updateBuilder.bindEntityIds("entityListTop", entityIds, uris);

        entityIds.addAll(lexemeSubIds);
        updateBuilder.bindEntityIds("entityList", entityIds, uris);
        updateBuilder.bindStatements("insertStatements", insertStatements);
        updateBuilder.bindValues("entityStatements", classifiedStatements.entityStatements);

        updateBuilder.bindValues("statementStatements", classifiedStatements.statementStatements);
        updateBuilder.bindValues("aboutStatements", classifiedStatements.aboutStatements);
        updateBuilder.bindValue("ts", timestamp);

        if (!refSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", refSet);
            // This is not necessary but easier than having separate templates
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            updateBuilder.bind("refCleanupQuery", cleanup.toString());
        }  else {
            updateBuilder.bind("refCleanupQuery", "");
        }

        if (!valueSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", valueSet);
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            updateBuilder.bind("valueCleanupQuery", cleanup.toString());
        }  else {
            updateBuilder.bind("valueCleanupQuery", "");
        }

        return updateBuilder.toString();
    }

    private static String getPrefilledTemplate(UrisScheme uris) {
        UpdateBuilder updateBuilder = new UpdateBuilder(loadBody("multiSync", MultiSyncUpdateQueryFactory.class));
        // Pre-bind static elements of the template
        updateBuilder.bindUri("schema:about", SchemaDotOrg.ABOUT);
        updateBuilder.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);
        updateBuilder.bind("uris.value", uris.value());
        updateBuilder.bind("uris.statement", uris.statement());
        return updateBuilder.toString();
    }
}
