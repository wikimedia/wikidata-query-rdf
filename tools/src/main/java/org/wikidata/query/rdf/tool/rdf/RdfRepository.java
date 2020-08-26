package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.stream.Collectors.toSet;
import static org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics.getMutationCountOnlyMetrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

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
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.Utils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.rdf.client.UpdateMetricsResponseHandler;

import com.google.common.collect.ImmutableSetMultimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for communicating with the RDF repository.
 */
public class RdfRepository {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);
    /**
     * How many statements we will send to RDF processor at once.
     * We assume typical triple line size is under 200 bytes.
     * Each statement appears twice in the output data. So that's how we derive the statement limit.
     * TODO: maybe we can do fine with just one limit?
     */
    private final long maxStatementsPerBatch;

    /**
     * Max statement data size.
     * Entity data repeats twice plus we're taking 1M safety buffer for other data.
     */
	private final long maxPostDataSize;

    /**
     * Uris for wikibase.
     */
    private final UrisScheme uris;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getValues;
    /**
     * SPARQL for a portion of the update.
     */
    private final String getRefs;
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
    /**
     * SPARQL to get lexeme sub-ids.
     */
    private final String getLexemes;

    protected final RdfClient rdfClient;
    private final MultiSyncUpdateQueryFactory multiSyncUpdateQueryFactory;

    /**
     * @param maxPostSize Max POST form content size.
     *                    Should be in sync with Jetty org.eclipse.jetty.server.Request.maxFormContentSize setting.
     *                    Production default is 200M, see runBlazegraph.sh file.
     *                    If that setting is changed, this one should change too, otherwise we get POST errors on big updates.
     *                    See: https://phabricator.wikimedia.org/T210235
     */
    public RdfRepository(UrisScheme uris, RdfClient rdfClient, long maxPostSize) {
        this.uris = uris;
        this.rdfClient = rdfClient;

        updateLeftOffTimeBody = loadBody("updateLeftOffTime");
        getValues = loadBody("GetValues");
        getRefs = loadBody("GetRefs");
        getRevisions = loadBody("GetRevisions");
        verify = loadBody("verify");
        getLexemes = loadBody("GetLexemes");
        maxStatementsPerBatch = maxPostSize / 400;
        maxPostDataSize = (maxPostSize - 1024 * 1024) / 2;
        multiSyncUpdateQueryFactory = new MultiSyncUpdateQueryFactory(uris);
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
        HashSet<String> values = new HashSet<>();
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

        return rdfClient.selectToMap(b.toString(), "entity", "s");
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

        return rdfClient.selectToMap(b.toString(), "entity", "s");
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
    public CollectedUpdateMetrics syncFromChanges(Collection<Change> changes, boolean verifyResult) {
        if (changes.isEmpty()) {
            // no changes, we're done
            return getMutationCountOnlyMetrics(0);
        }

        Set<String> entityIds = newHashSetWithExpectedSize(changes.size());
        List<Statement> insertStatements = new ArrayList<>();
        ClassifiedStatements classifiedStatements = new ClassifiedStatements(uris);

        Set<String> valueSet = new HashSet<>();
        Set<String> refSet = new HashSet<>();

        CollectedUpdateMetrics collectedMetrics = new CollectedUpdateMetrics();
        for (final Change change : changes) {
            if (change.getStatements() == null) {
                // broken change, probably failed retrieval
                continue;
            }
            entityIds.add(change.entityId());
            insertStatements.addAll(change.getStatements());
            classifiedStatements.classify(change.getStatements(), change.entityId());
            valueSet.addAll(change.getValueCleanupList());
            refSet.addAll(change.getRefCleanupList());
            // If current batch data has grown too big, we send it out and start the new one.
            if (insertStatements.size() > maxStatementsPerBatch || classifiedStatements.getDataSize() > maxPostDataSize) {
                // Send the batch out and clean up
                // Logging as info for now because I want to know how many split batches we get. I don't want too many.
                log.info("Too much data with {} bytes - sending batch out, last ID {}", classifiedStatements.getDataSize(), change.entityId());

                collectedMetrics.merge(sendUpdateBatch(entityIds, insertStatements, classifiedStatements,
                        valueSet, refSet, verifyResult));
                entityIds.clear();
                insertStatements.clear();
                classifiedStatements.clear();
                valueSet.clear();
                refSet.clear();
            }
        }

        if (!entityIds.isEmpty()) {
            collectedMetrics.merge(sendUpdateBatch(entityIds,
                                        insertStatements, classifiedStatements,
                    valueSet, refSet, verifyResult));
        }

        return collectedMetrics;
    }

    private CollectedUpdateMetrics sendUpdateBatch(Set<String> entityIds,
                                List<Statement> insertStatements,
                                ClassifiedStatements classifiedStatements,
                                Set<String> valueSet,
                                Set<String> refSet,
                                boolean verifyResult) {
        log.debug("Processing {} IDs and {} statements", entityIds.size(), insertStatements.size());

        String query = multiSyncUpdateQueryFactory.buildQuery(entityIds,
                insertStatements,
                classifiedStatements,
                valueSet,
                refSet,
                fetchLexemeSubIds(entityIds),
                Instant.now());

        log.debug("Sending query {} bytes", query.length());
        long start = System.nanoTime();
        CollectedUpdateMetrics collectedUpdateMetrics = rdfClient.update(query, new UpdateMetricsResponseHandler(!refSet.isEmpty(), !valueSet.isEmpty()));
        log.debug("Update query took {} nanos and modified {} statements",
                System.nanoTime() - start, collectedUpdateMetrics.getMutationCount());

        if (verifyResult) {
            try {
                verifyStatements(entityIds, insertStatements);
            } catch (QueryEvaluationException e) {
                throw new FatalException("Can't load verify results: " + e, e);
            }
        }

        return collectedUpdateMetrics;
    }

    /**
     * Fetch sub-ids for given lexeme entity IDs.
     * We need them because forms & senses have statements too.
     * @param entityIds Set of parent entity IDs.
     * @return List of IDs for forms and senses.
     */
    private List<String> fetchLexemeSubIds(Set<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getLexemes);
        b.bindEntityIds("entityList", entityIds, uris);
        return rdfClient.getEntityIds(b.toString(), "lex", uris);
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
        bv.bindEntityIds("entityList", entityIds, uris);
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
     * Filter set of changes and see which of them really need to be updated.
     * The changes that have their revision or better in the repo do not need update.
     * @param candidates List of candidate changes
     * @return Set of entity IDs for which the update is needed.
     */
    public Set<String> hasRevisions(Collection<Change> candidates) {
        UpdateBuilder b = new UpdateBuilder(getRevisions);
        StringBuilder values = new StringBuilder();
        for (Change entry: candidates) {
            values.append("( <").append(uris.entityIdToURI(entry.entityId())).append("> ")
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
        // Only use the earliest TS from the dump since
        b.append("SELECT * WHERE { ontology:Dump schema:dateModified ?date } ORDER BY ASC(?date) LIMIT 1");
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
     * Detects the values that are no longer referenced from the entity statements.
     * @param existingValues the existing values as known by the rdf repository (old state)
     * @param entityStatements the statements of the entity (new state before munging)
     * @return A list of potential orphaned value ids
     */
    public static Set<String> extractValuesToCleanup(Set<String> existingValues, Collection<Statement> entityStatements) {
        return extractSubjectsToCleanup(existingValues, entityStatements, StatementPredicates::valueTypeStatement);
    }

    /**
     * Detects the references that are no longer referenced from the entity statements.
     * @param existingReferences the existing referenced as known by the rdf repository (old state)
     * @param entityStatements the statements of the entity (new state before munging)
     * @return A list of potential orphaned reference ids
     */
    public static Set<String> extractReferencesToCleanup(Set<String> existingReferences, Collection<Statement> entityStatements) {
        return extractSubjectsToCleanup(existingReferences, entityStatements, StatementPredicates::referenceTypeStatement);
    }

    private static Set<String> extractSubjectsToCleanup(Set<String> existingSubjects, Collection<Statement> entityStatements,
                                                        Predicate<Statement> subjectFilter) {
        if (existingSubjects.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> newStateSubjects = entityStatements.stream()
            .filter(subjectFilter)
            .map(triple -> triple.getSubject().stringValue())
            .collect(toSet());
        return difference(existingSubjects, newStateSubjects);
    }
}
