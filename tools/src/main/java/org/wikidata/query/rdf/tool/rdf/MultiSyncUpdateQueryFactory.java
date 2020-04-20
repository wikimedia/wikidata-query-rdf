package org.wikidata.query.rdf.tool.rdf;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

final class MultiSyncUpdateQueryFactory {

    private final UrisScheme uris;

    MultiSyncUpdateQueryFactory(UrisScheme uris) {
        this.uris = uris;
    }

    @SuppressFBWarnings(value = "OCP_OVERLY_CONCRETE_PARAMETER", justification = "It isn't entirely clear if order is important or not in lexemeSubIds.")
    String buildQuery(
            Set<String> entityIds,
                      List<Statement> insertStatements,
                      ClassifiedStatements classifiedStatements,
                      Set<String> valueSet,
                      Set<String> refSet,
                      List<String> lexemeSubIds,
                      Instant timestamp) {

        Set<String> entityIdsWithLexemes = new LinkedHashSet<>();
        entityIdsWithLexemes.addAll(entityIds);
        entityIdsWithLexemes.addAll(lexemeSubIds);

        return Arrays.stream(MultiSyncStep.values())
                .filter(step -> step != MultiSyncStep.CLEANUP_REFERENCES || !refSet.isEmpty())
                .filter(step -> step != MultiSyncStep.CLEANUP_VALUES || !valueSet.isEmpty())
                .map(step -> prepareQuery(step,
                        entityIds,
                        entityIdsWithLexemes,
                        insertStatements,
                        classifiedStatements,
                        refSet,
                        valueSet,
                        timestamp))
                .collect(Collectors.joining("\n"));
    }

    private String prepareQuery(MultiSyncStep step,
                                Collection<String> entityTopIds,
                                Collection<String> entityIdsWithLexemes,
                                List<Statement> insertStatements,
                                ClassifiedStatements classifiedStatements,
                                Set<String> refSet,
                                Set<String> valueSet,
                                Instant timestamp) {
        switch (step) {
            case CLEAR_OOD_SITE_LINKS:
                return MultiSyncStep.createClearOodLinksQuery(entityTopIds, classifiedStatements, uris);
            case CLEAR_OOD_ST_ABOUT_ST:
                return MultiSyncStep.createClearOodStatementsAboutStatementsQuery(entityIdsWithLexemes, classifiedStatements, uris);
            case CLEAR_OOD_ST_ABOUT_ENT:
                return MultiSyncStep.createClearOodStatementsAboutEntitiesQuery(entityIdsWithLexemes, classifiedStatements, uris);
            case INSERT_NEW_DATA:
                return MultiSyncStep.createInsertNewDataQuery(insertStatements);
            case ADD_TIMESTAMPS:
                return MultiSyncStep.createAddTimestampsQuery(entityTopIds, timestamp, uris);
            case CLEANUP_REFERENCES:
                return MultiSyncStep.createCleanupReferencesQuery(refSet);
            case CLEANUP_VALUES:
                return MultiSyncStep.createCleanupValuesQuery(valueSet);
            default:
                throw new IllegalArgumentException("Step " + step.getStepName() + " is unknown!");
        }
    }
}
