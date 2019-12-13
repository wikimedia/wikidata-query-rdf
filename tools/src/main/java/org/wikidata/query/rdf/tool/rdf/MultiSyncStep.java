package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.Utils;

public enum MultiSyncStep {

    CLEAR_OOD_SITE_LINKS("clearOodSiteLinks"),
    CLEAR_OOD_ST_ABOUT_ST("clearOodStatementsAboutStatements"),
    CLEAR_OOD_ST_ABOUT_ENT("clearOodStatementsAboutEntities"),
    INSERT_NEW_DATA("insertNewData"),
    ADD_TIMESTAMPS("addTimestamps"),
    CLEANUP_REFERENCES("cleanupReferences", "cleanUnused"),
    CLEANUP_VALUES("cleanupValues", "cleanUnused");

    private final String body;
    private final String metricBaseName;
    private String stepName;

    MultiSyncStep(String stepName) {
        this(stepName, stepName);
    }

    MultiSyncStep(String stepName, String templateName) {
        this.body = Utils.loadBody(templateName, MultiSyncStep.class).trim();
        this.stepName = stepName;
        this.metricBaseName = "blazegraph-" + LOWER_CAMEL.to(LOWER_HYPHEN, stepName).replaceAll("_", "-") + "-";
    }

    public String getStepName() {
        return stepName;
    }

    public String getMetricBaseName() {
        return metricBaseName;
    }

    private String getBody() {
        return body;
    }

    public static String createClearOodLinksQuery(Collection<String> entityTopIds,
                                                  ClassifiedStatements classifiedStatements,
                                                  UrisScheme uris) {
        return getUpdateBuilder(CLEAR_OOD_SITE_LINKS)
                .bindUri("schema:about", SchemaDotOrg.ABOUT)
                .bindEntityIds("entityListTop", entityTopIds, uris)
                .bindValues("aboutStatements", classifiedStatements.aboutStatements)
                .toString();
    }

    public static String createClearOodStatementsAboutStatementsQuery(Collection<String> entityIds,
                                                                      ClassifiedStatements classifiedStatements,
                                                                      UrisScheme uris) {
        return getUpdateBuilder(CLEAR_OOD_ST_ABOUT_ST)
                .bindEntityIds("entityList", entityIds, uris)
                .bind("uris.statement", uris.statement())
                .bindValues("statementStatements", classifiedStatements.statementStatements)
                .toString();
    }

    public static String createClearOodStatementsAboutEntitiesQuery(Collection<String> entityIds,
                                                                    ClassifiedStatements classifiedStatements,
                                                                    UrisScheme uris) {
        return getUpdateBuilder(CLEAR_OOD_ST_ABOUT_ENT)
            .bindEntityIds("entityList", entityIds, uris)
            .bindValues("entityStatements", classifiedStatements.entityStatements)
            .toString();
    }

    public static String createInsertNewDataQuery(List<Statement> insertStatements) {
        return getUpdateBuilder(INSERT_NEW_DATA)
                .bindStatements("insertStatements", insertStatements)
                .toString();
    }

    public static String createAddTimestampsQuery(Collection<String> entityTopIds, Instant timestamp, UrisScheme uris) {
        return getUpdateBuilder(ADD_TIMESTAMPS)
                .bindValue("ts", timestamp)
                .bindEntityIds("entityListTop", entityTopIds, uris)
                .toString();
    }

    public static String createCleanupReferencesQuery(Set<String> refSet) {
        return getUpdateBuilder(CLEANUP_REFERENCES)
                .bindUris("values", refSet)
                .bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED)
                .toString();
    }

    public static String createCleanupValuesQuery(Set<String> valueSet) {
        return getUpdateBuilder(CLEANUP_VALUES)
                .bindUris("values", valueSet)
                .bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED)
                .toString();
    }

    private static UpdateBuilder getUpdateBuilder(MultiSyncStep step) {
        return new UpdateBuilder(step.getBody());
    }
}
