package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSetMultimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for communicating with the RDF repository.
 */
@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "spotbug limitation: https://github.com/spotbugs/spotbugs/issues/463")
public class RdfRepository {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);

    /**
     * Uris for wikibase.
     */
    private final WikibaseUris uris;

    /**
     * SPARQL for a portion of the update.
     */
    private final String syncBody;
    /**
     * SPARQL for a portion of the update, batched sync.
     */
    private final String msyncBody;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getValues;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getRefs;
    /**
     * SPARQL for a portion of the update.
     */
    private final String cleanUnused;
    /**
     * SPARQL to sync the left off time.
     */
    private final String updateLeftOffTimeBody;
    /**
     * SPARQL to filter entities for newer revisions.
     */
    private final String getRevisions;
    /**
     * SPARQL to verify update worked.
     */
    private final String verify;

    @VisibleForTesting
    protected final RdfClient rdfClient;

    public RdfRepository(WikibaseUris uris, RdfClient rdfClient) {
        this.uris = uris;
        this.rdfClient = rdfClient;

        msyncBody = loadBody("multiSync");
        syncBody = loadBody("sync");
        updateLeftOffTimeBody = loadBody("updateLeftOffTime");
        getValues = loadBody("GetValues");
        getRefs = loadBody("GetRefs");
        cleanUnused = loadBody("CleanUnused");
        getRevisions = loadBody("GetRevisions");
        verify = loadBody("verify");
    }

    /**
     * Loads some sparql.
     *
     * @param name name of the sparql file to load - the actual file loaded is
     *            RdfRepository.%name%.sparql.
     * @return contents of the sparql file
     * @throws FatalException if there is an error loading the file
     */
    private static String loadBody(String name) {
        return Utils.loadBody(name, RdfRepository.class);
    }

    /**
     * Collect results of the query into string set.
     *
     * @param result Result object
     * @param binding Binding name to collect
     * @return Collection of strings resulting from the query.
     */
    private Set<String> resultToSet(TupleQueryResult result, String binding) {
        HashSet<String> values = new HashSet<String>();
        try {
            while (result.hasNext()) {
                Binding value = result.next().getBinding(binding);
                if (value == null) {
                    continue;
                }
                values.add(value.getValue().stringValue());
            }
        } catch (QueryEvaluationException e) {
            throw new FatalException("Can't load results: " + e, e);
        }
        return values;
    }

    /**
     * Collect results of the query into a multimap by first parameter.
     *
     * @param result Result object
     * @param keyBinding Binding name to serve as key
     * @param valueBinding Binding name to serve as values
     * @return Collection of strings resulting from the query.
     */
    public ImmutableSetMultimap<String, String> resultToMap(TupleQueryResult result, String keyBinding, String valueBinding) {
        ImmutableSetMultimap.Builder<String, String> values = ImmutableSetMultimap.builder();
        try {
            while (result.hasNext()) {
                BindingSet bindings = result.next();
                Binding value = bindings.getBinding(valueBinding);
                Binding key = bindings.getBinding(keyBinding);
                if (value == null || key == null) {
                    continue;
                }
                values.put(key.getValue().stringValue(), value.getValue().stringValue());
            }
        } catch (QueryEvaluationException e) {
            throw new FatalException("Can't load results: " + e, e);
        }
        return values.build();
    }

    /**
     * Perform a SPARQL query and return the result as a map.
     * @param query SPARQL query, should be SELECT
     * @param keyBinding Binding name to serve as key
     * @param valueBinding Binding name to serve as values
     * @return Collection of strings resulting from the query.
     */
    public ImmutableSetMultimap<String, String> selectToMap(String query, String keyBinding, String valueBinding) {
        return resultToMap(rdfClient.query(query), keyBinding, valueBinding);
    }


    /**
     * Get list of value subjects connected to entity. The connection is either
     * via statement or via reference or via qualifier.
     *
     * @return Set of value subjects
     */
    public ImmutableSetMultimap<String, String> getValues(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getValues);
        b.bindUris("entityList", entityIds);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToMap(rdfClient.query(b.toString()), "entity", "s");
    }

    /**
     * Get list of reference subjects connected to entity.
     *
     * @return Set of references
     */
    public ImmutableSetMultimap<String, String> getRefs(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getRefs);
        b.bindUris("entityList", entityIds);
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToMap(rdfClient.query(b.toString()), "entity", "s");
    }

    /**
     * Provides the SPARQL needed to synchronize the data statements for a single entity.
     *
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     * @param valueList list of used values, for cleanup
     * @return the number of statements modified
     */
    private String getSyncQuery(String entityId, Collection<Statement> statements, Collection<String> valueList) {
        // TODO this is becoming a mess too
        log.debug("Generating update for {}", entityId);
        UpdateBuilder b = new UpdateBuilder(syncBody);
        b.bindUri("entity:id", uris.entity() + entityId);
        b.bindUri("schema:about", SchemaDotOrg.ABOUT);
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindStatements("insertStatements", statements);

        List<Statement> entityStatements = new ArrayList<>();
        List<Statement> statementStatements = new ArrayList<>();
        Set<Statement> aboutStatements = new HashSet<>();
        classifyStatements(statements, entityId, entityStatements, statementStatements, aboutStatements);

        b.bindValues("entityStatements", entityStatements);
        b.bindValues("statementStatements", statementStatements);
        b.bindValues("aboutStatements", aboutStatements);

        if (valueList != null && !valueList.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", valueList);
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            b.bind("cleanupQuery", cleanup.toString());
        }  else {
            b.bind("cleanupQuery", "");
        }

        return b.toString();
    }

    /**
     * Sort statements into a set of specialized collections, by subject.
     * @param statements List of statements to process
     * @param entityId
     * @param entityStatements subject is entity
     * @param statementStatements subject is any statement
     * @param aboutStatements not entity, not statement, not value and not reference
     */
    private void classifyStatements(Collection<Statement> statements,
            String entityId, Collection<Statement> entityStatements,
            Collection<Statement> statementStatements,
            Collection<Statement> aboutStatements) {
        for (Statement statement: statements) {
            String s = statement.getSubject().stringValue();
            if (s.equals(uris.entity() + entityId)) {
                entityStatements.add(statement);
            }
            if (s.startsWith(uris.statement())) {
                statementStatements.add(statement);
            }
            if (!s.equals(uris.entity() + entityId)
                    && !s.startsWith(uris.statement())
                    && !s.startsWith(uris.value())
                    && !s.startsWith(uris.reference())
            ) {
                aboutStatements.add(statement);
            }
        }
    }

    /**
     * Sync repository from changes list.
     *
     * Synchronizes the RDF repository's representation of an entity to be
     * exactly the provided statements. You can think of the RDF managed for an
     * entity as a tree rooted at the entity. The managed tree ends where the
     * next entity's managed tree starts. For example Q23 from wikidata includes
     * all statements about George Washington but not those about Martha
     * (Q191789) even though she is linked by the spouse attribute. On the other
     * hand the qualifiers on statements about George are included in George.
     *
     * @param changes List of changes.
     * @return Number of triples modified.
     */
    public int syncFromChanges(Collection<Change> changes, boolean verifyResult) {
        if (changes.isEmpty()) {
            // no changes, we're done
            return 0;
        }
        UpdateBuilder b = new UpdateBuilder(msyncBody);
        b.bindUri("schema:about", SchemaDotOrg.ABOUT);
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        Set<String> entityIds = newHashSetWithExpectedSize(changes.size());

        List<Statement> insertStatements = new ArrayList<>();
        List<Statement> entityStatements = new ArrayList<>();
        List<Statement> statementStatements = new ArrayList<>();
        Set<Statement> aboutStatements = new HashSet<>();
        Set<String> valueSet = new HashSet<>();
        Set<String> refSet = new HashSet<>();

        for (final Change change : changes) {
            if (change.getStatements() == null) {
                // broken change, probably failed retrieval
                continue;
            }
            entityIds.add(change.entityId());
            insertStatements.addAll(change.getStatements());
            classifyStatements(change.getStatements(), change.entityId(), entityStatements, statementStatements, aboutStatements);
            valueSet.addAll(change.getValueCleanupList());
            refSet.addAll(change.getRefCleanupList());
        }

        if (entityIds.isEmpty()) {
            // If we've got no IDs, this means all change retrieval failed
            log.debug("Got no valid changes, we're done");
            return 0;
        }

        b.bindUris("entityList", entityIds, uris.entity());
        b.bindStatements("insertStatements", insertStatements);
        b.bindValues("entityStatements", entityStatements);

        b.bindValues("statementStatements", statementStatements);
        b.bindValues("aboutStatements", aboutStatements);

        if (!refSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", refSet);
            // This is not necessary but easier than having separate templates
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            b.bind("refCleanupQuery", cleanup.toString());
        }  else {
            b.bind("refCleanupQuery", "");
        }

        if (!valueSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", valueSet);
            cleanup.bindUri("wikibase:quantityNormalized", Ontology.Quantity.NORMALIZED);
            b.bind("valueCleanupQuery", cleanup.toString());
        }  else {
            b.bind("valueCleanupQuery", "");
        }

        long start = System.currentTimeMillis();
        int modified = rdfClient.update(b.toString());
        log.debug("Update query took {} millis and modified {} statements",
                System.currentTimeMillis() - start, modified);

        if (verifyResult) {
            try {
                verifyStatements(entityIds, insertStatements);
            } catch (QueryEvaluationException e) {
                throw new FatalException("Can't load verify results: " + e, e);
            }
        }

        return modified;
    }

    /**
     * Verify that the database matches the statement data for these IDs.
     * @param entityIds List of IDs
     * @param statements List of statements for these IDs
     * @throws QueryEvaluationException if there is a problem retrieving result.
     */
    @SuppressFBWarnings(value = "SLF4J_SIGN_ONLY_FORMAT", justification = "We rely on that format.")
    private void verifyStatements(Set<String> entityIds, List<Statement> statements)
            throws QueryEvaluationException {
        log.debug("Verifying the update");
        UpdateBuilder bv = new UpdateBuilder(verify);
        bv.bindUri("schema:about", SchemaDotOrg.ABOUT);
        bv.bind("uris.statement", uris.statement());
        bv.bindUris("entityList", entityIds, uris.entity());
        bv.bindValues("allStatements", statements);
        TupleQueryResult result = rdfClient.query(bv.toString());
        if (result.hasNext()) {
            log.error("Update failed, we have extra data!");
            while (result.hasNext()) {
                BindingSet bindings = result.next();
                Binding s = bindings.getBinding("s");
                Binding p = bindings.getBinding("p");
                Binding o = bindings.getBinding("o");
                log.error("{}\t{}\t{}", s.getValue().stringValue(),
                        p.getValue().stringValue(), o.getValue().stringValue());
            }
            throw new FatalException("Update failed, bad old data in the store");
        }
        log.debug("Verification OK");
    }

    /**
     * Synchronizes the RDF repository's representation of an entity to be
     * exactly the provided statements. You can think of the RDF managed for an
     * entity as a tree rooted at the entity. The managed tree ends where the
     * next entity's managed tree starts. For example Q23 from wikidata includes
     * all statements about George Washington but not those about Martha
     * (Q191789) even though she is linked by the spouse attribute. On the other
     * hand the qualifiers on statements about George are included in George.
     *
     * This method is not used for actual updates but is used for tests.
     * TODO: switch tests to use same method as actual updates do
     *
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     * @param valueList list of used values, for cleanup
     * @return the number of statements modified
     */
    public int sync(String entityId, Collection<Statement> statements, Collection<String> valueList) {
        long start = System.currentTimeMillis();
        int modified = rdfClient.update(getSyncQuery(entityId, statements, valueList));
        log.debug("Updating {} took {} millis and modified {} statements", entityId,
                System.currentTimeMillis() - start, modified);
        return modified;
    }

    /**
     * Synchronizes the RDF repository's representation.
     *
     * This method is not used for actual updates but is used for tests.
     * TODO: switch tests to use same method as actual updates do
     *
     * @see #sync(String, Collection, Collection)
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     * @return the number of statements modified
     */
    public int sync(String entityId, Collection<Statement> statements) {
        return sync(entityId, statements, null);
    }

    /**
     * Filter set of changes and see which of them really need to be updated.
     * The changes that have their revision or better in the repo do not need update.
     * @param candidates List of candidate changes
     * @return Set of entity IDs for which the update is needed.
     */
    public Set<String> hasRevisions(Collection<Change> candidates) {
        UpdateBuilder b = new UpdateBuilder(getRevisions);
        StringBuilder values = new StringBuilder();
        for (Change entry: candidates) {
            values.append("( <").append(uris.entity()).append(entry.entityId()).append("> ")
                    .append(entry.revision()).append(" )\n");
        }
        b.bind("values", values.toString());
        b.bindUri("schema:version", SchemaDotOrg.VERSION);
        return resultToSet(rdfClient.query(b.toString()), "s");
    }

    /**
     * Does the triple store have this revision or better.
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "we want to be platform independent here.")
    public boolean hasRevision(String entityId, long revision) {
        return rdfClient.ask(String.format(Locale.ROOT,
                "ASK {\n wd:%s schema:version ?v .\n  FILTER (?v >= %s)\n}",
                entityId, revision));
    }

    /**
     * Fetch where we left off updating the repository.
     *
     * @return the date or null if we have nowhere to start from
     */
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "prefix() is called with different StringBuilders")
    public Instant fetchLeftOffTime() {
        log.info("Checking for left off time from the updater");
        StringBuilder b = SchemaDotOrg.prefix(new StringBuilder());
        b.append("SELECT * WHERE { <").append(uris.root()).append("> schema:dateModified ?date }");
        Instant leftOffTime = dateFromQuery(b.toString());
        if (leftOffTime != null) {
            log.info("Found left off time from the updater");
            return leftOffTime;
        }
        log.info("Checking for left off time from the dump");
        b = Ontology.prefix(SchemaDotOrg.prefix(new StringBuilder()));
        b.append("SELECT * WHERE { ontology:Dump schema:dateModified ?date }");
        return dateFromQuery(b.toString());
    }

    /**
     * Update where we left off so when fetchLeftOffTime is next called it
     * returns leftOffTime so we can continue from there after the updater is
     * restarted.
     */
    public void updateLeftOffTime(Instant leftOffTime) {
        log.debug("Setting last updated time to {}", leftOffTime);
        UpdateBuilder b = new UpdateBuilder(updateLeftOffTimeBody);
        b.bindUri("root", uris.root());
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        b.bindValue("date", leftOffTime);
        rdfClient.update(b.toString());
    }

    /**
     * Run a query that returns just a date in the "date" binding and return its
     * result.
     */
    private Instant dateFromQuery(String query) {
        TupleQueryResult result = rdfClient.query(query);
        try {
            if (!result.hasNext()) {
                return null;
            }
            Binding maxLastUpdate = result.next().getBinding("date");
            if (maxLastUpdate == null) {
                return null;
            }
            // Note that XML calendar and Instant have the same default format
            XMLGregorianCalendar xmlCalendar = ((Literal) maxLastUpdate.getValue()).calendarValue();
            /*
             * We convert rather blindly to a GregorianCalendar because we're
             * reasonably sure all the right data is present.
             */
            GregorianCalendar calendar = xmlCalendar.toGregorianCalendar();
            return calendar.getTime().toInstant();
        } catch (QueryEvaluationException e) {
            throw new FatalException("Error evaluating query", e);
        }
    }

    /**
     * Generic update method.
     * @param sparql SPARQL UPDATE query
     * @return How many triples were mutated.
     */
    public Integer updateQuery(String sparql) {
        return rdfClient.update(sparql);
    }
}
