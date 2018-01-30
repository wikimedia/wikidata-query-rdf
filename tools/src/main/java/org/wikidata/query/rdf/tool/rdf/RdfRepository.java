package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static com.google.common.io.Resources.getResource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.binary.BinaryQueryResultParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.io.Resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for communicating with the RDF repository.
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "spotbug limitation: https://github.com/spotbugs/spotbugs/issues/463")
public class RdfRepository implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);
    /**
     * UTC timezone.
     */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    /**
     * Http connection pool for the rdf repository.
     */
    private final HttpClient httpClient;

    /**
     * URI for the wikibase rdf repository.
     */
    private final URI uri;
    /**
     * Uris for wikibase.
     */
    private final WikibaseUris uris;

    // SPARQL queries
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

    /**
     * How many times we retry a failed HTTP call.
     */
    private int maxRetries = 6;
    /**
     * How long to delay after failing first HTTP call, in milliseconds.
     * Next retries would be slower exponentially by 2x until maxRetries is exhausted.
     * Note that the first retry is 2x delay due to the way Retryer is implemented.
     */
    private int delay = 1000;

    /**
     * Configuration name for proxy host.
     */
    private static final String HTTP_PROXY = "http.proxyHost";
    /**
     * Configuration name for proxy port.
     */
    private static final String HTTP_PROXY_PORT = "http.proxyPort";

    /**
     * Request timeout property.
     */
    public static final String TIMEOUT_PROPERTY = RdfRepository.class + ".timeout";

    /**
     * Request timeout.
     */
    private final int timeout;

    /**
     * Retryer for fetching data from RDF store.
     */
    private final Retryer<ContentResponse> retryer;

    public RdfRepository(URI uri, WikibaseUris uris) {
        this.uri = uri;
        this.uris = uris;
        msyncBody = loadBody("multiSync");
        syncBody = loadBody("sync");
        updateLeftOffTimeBody = loadBody("updateLeftOffTime");
        getValues = loadBody("GetValues");
        getRefs = loadBody("GetRefs");
        cleanUnused = loadBody("CleanUnused");
        getRevisions = loadBody("GetRevisions");
        verify = loadBody("verify");

        timeout = Integer.parseInt(System.getProperty(TIMEOUT_PROPERTY, "-1"));
        httpClient = new HttpClient(new SslContextFactory(true/* trustAll */));
        setupHttpClient();

        retryer = RetryerBuilder.<ContentResponse>newBuilder()
                .retryIfExceptionOfType(TimeoutException.class)
                .retryIfExceptionOfType(ExecutionException.class)
                .retryIfExceptionOfType(IOException.class)
                .retryIfRuntimeException()
                .withWaitStrategy(WaitStrategies.exponentialWait(delay, 10, TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(maxRetries))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        if (attempt.hasException()) {
                            log.info("HTTP request failed: {}, attempt {}, will {}",
                                    attempt.getExceptionCause(),
                                    attempt.getAttemptNumber(),
                                    attempt.getAttemptNumber() < maxRetries ? "retry" : "fail");
                        }
                    }
                })
                .build();
    }

    /**
     * Setup HTTP client settings.
     */
    @SuppressWarnings("checkstyle:illegalcatch")
    private void setupHttpClient() {
        if (System.getProperty(HTTP_PROXY) != null
                && System.getProperty(HTTP_PROXY_PORT) != null) {
            final ProxyConfiguration proxyConfig = httpClient.getProxyConfiguration();
            final HttpProxy proxy = new HttpProxy(
                    System.getProperty(HTTP_PROXY),
                    Integer.parseInt(System.getProperty(HTTP_PROXY_PORT)));
            proxy.getExcludedAddresses().add("localhost");
            proxy.getExcludedAddresses().add("127.0.0.1");
            proxyConfig.getProxies().add(proxy);
        }
        try {
            httpClient.start();
        // Who would think declaring it as throws Exception is a good idea?
        } catch (Exception e) {
            throw new RuntimeException("Unable to start HttpClient", e);
        }
    }

    /**
     * Close the repository.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        httpClient.stop();
    }

    /**
     * Get max retries count.
     * @return How many times we retry a failed HTTP call.
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set how many times we retry a failed HTTP call.
     * @return this
     */
    public RdfRepository setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Get retry delay.
     * @return How long to delay after failing first HTTP call, in milliseconds.
     */
    public int getDelay() {
        return delay;
    }

    /**
     * Set retry delay.
     * Specifies how long to delay after failing first HTTP call, in milliseconds.
     * Next retries would be slower by 2x, 3x, 4x etc. until maxRetries is exhausted.
     * @return The repository object
     */
    public RdfRepository setDelay(int delay) {
        this.delay = delay;
        return this;
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
        URL url = getResource(RdfRepository.class, "RdfRepository." + name + ".sparql");
        try {
            return Resources.toString(url, Charsets.UTF_8);
        } catch (IOException e) {
            throw new FatalException("Can't load " + url, e);
        }
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
    private ImmutableSetMultimap<String, String> resultToMap(TupleQueryResult result, String keyBinding, String valueBinding) {
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
     * Get list of value subjects connected to entity. The connection is either
     * via statement or via reference or via qualifier.
     *
     * @param entityIds
     * @return Set of value subjects
     */
    public ImmutableSetMultimap<String, String> getValues(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getValues);
        b.bindUris("entityList", entityIds);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToMap(query(b.toString()), "entity", "s");
    }

    /**
     * Get list of reference subjects connected to entity.
     *
     * @param entityIds
     * @return Set of references
     */
    public ImmutableSetMultimap<String, String> getRefs(Collection<String> entityIds) {
        UpdateBuilder b = new UpdateBuilder(getRefs);
        b.bindUris("entityList", entityIds);
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToMap(query(b.toString()), "entity", "s");
    }

    /**
     * Provides the SPARQL needed to synchronize the data statements for a single entity.
     *
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     * @param valueList list of used values, for cleanup
     * @return the number of statements modified
     */
    public String getSyncQuery(String entityId, Collection<Statement> statements, Collection<String> valueList) {
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

        for (final Change change : changes) {
            if (change.getStatements() == null) {
                // broken change, probably failed retrieval
                continue;
            }
            entityIds.add(change.entityId());
            insertStatements.addAll(change.getStatements());
            classifyStatements(change.getStatements(), change.entityId(), entityStatements, statementStatements, aboutStatements);
            valueSet.addAll(change.getCleanupList());
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

        if (!valueSet.isEmpty()) {
            UpdateBuilder cleanup = new UpdateBuilder(cleanUnused);
            cleanup.bindUris("values", valueSet);
            b.bind("cleanupQuery", cleanup.toString());
        }  else {
            b.bind("cleanupQuery", "");
        }

        long start = System.currentTimeMillis();
        int modified = execute("update", UPDATE_COUNT_RESPONSE, b.toString());
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
        TupleQueryResult result = query(bv.toString());
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
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     * @param valueList list of used values, for cleanup
     * @return the number of statements modified
     */
    public int sync(String entityId, Collection<Statement> statements, Collection<String> valueList) {
        long start = System.currentTimeMillis();
        int modified = execute("update", UPDATE_COUNT_RESPONSE, getSyncQuery(entityId, statements, valueList));
        log.debug("Updating {} took {} millis and modified {} statements", entityId,
                System.currentTimeMillis() - start, modified);
        return modified;
    }

    /**
     * Synchronizes the RDF repository's representation of an entity to be
     * exactly the provided statements.
     *
     * @param query Query text
     * @return the number of statements modified
     */
    public int syncQuery(String query) {
        long start = System.currentTimeMillis();
        int modified = execute("update", UPDATE_COUNT_RESPONSE, query);
        log.debug("Update query took {} millis and modified {} statements",
                System.currentTimeMillis() - start, modified);
        return modified;

    }

    /**
     * Synchronizes the RDF repository's representation.
     * See also: sync(String, Collection<Statement>, Collection<String>)
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
        return resultToSet(query(b.toString()), "s");
    }

    /**
     * Does the triple store have this revision or better.
     */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "we want to be platform independent here.")
    public boolean hasRevision(String entityId, long revision) {
        // TODO building queries with strings sucks because escaping....
        StringBuilder prefixes = new StringBuilder();
        prefixes.append("PREFIX schema: <").append(SchemaDotOrg.NAMESPACE).append(">\n");
        prefixes.append("PREFIX entity: <").append(uris.entity()).append(">\n");
        return ask(String.format(Locale.ROOT, "%sASK {\n  entity:%s schema:version ?v .\n  FILTER (?v >= %s)\n}",
                prefixes, entityId, revision));
    }

    /**
     * Fetch where we left off updating the repository.
     *
     * @return the date or null if we have nowhere to start from
     */
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "prefix() is called with different StringBuilders")
    public Date fetchLeftOffTime() {
        log.info("Checking for left off time from the updater");
        StringBuilder b = SchemaDotOrg.prefix(new StringBuilder());
        b.append("SELECT * WHERE { <").append(uris.root()).append("> schema:dateModified ?date }");
        Date leftOffTime = dateFromQuery(b.toString());
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
    public void updateLeftOffTime(Date leftOffTime) {
        log.debug("Setting last updated time to {}", leftOffTime);
        UpdateBuilder b = new UpdateBuilder(updateLeftOffTimeBody);
        b.bindUri("root", uris.root());
        b.bindUri("dateModified", SchemaDotOrg.DATE_MODIFIED);
        GregorianCalendar c = new GregorianCalendar(UTC, Locale.ROOT);
        c.setTime(leftOffTime);
        try {
            b.bindValue("date", DatatypeFactory.newInstance().newXMLGregorianCalendar(c));
        } catch (DatatypeConfigurationException e) {
            throw new FatalException("Holy cow datatype configuration exception on default "
                    + "datatype factory.  Seems like something really really strange.", e);
        }
        execute("update", UPDATE_COUNT_RESPONSE, b.toString());
    }

    /**
     * Execute a SPARQL ask and parse the boolean result.
     */
    public boolean ask(String sparql) {
        return execute("query", ASK_QUERY_RESPONSE, sparql);
    }

    /**
     * Execute some SPARQL which returns a results table.
     */
    public TupleQueryResult query(String sparql) {
        return execute("query", TUPLE_QUERY_RESPONSE, sparql);
    }

    /**
     * Create HTTP request.
     * @param type Request type
     * @param sparql SPARQL code
     * @param accept Accept header (can be null)
     * @return Request object
     */
    private Request makeRequest(String type, String sparql, String accept) {
        Request post = httpClient.newRequest(uri);
        post.method(HttpMethod.POST);
        if (timeout > 0) {
            post.timeout(timeout, TimeUnit.SECONDS);
        }
        // Note that Blazegraph totally ignores the Accept header for SPARQL
        // updates so the response is just html in that case...
        if (accept != null) {
            post.header("Accept", accept);
        }

        final Fields fields = new Fields();
        fields.add(type, sparql);
        final FormContentProvider form = new FormContentProvider(fields, Charsets.UTF_8);
        post.content(form);
        return post;
    }

    /**
     * Execute some raw SPARQL.
     *
     * @param type name of the parameter in which to send sparql
     * @return results string from the server
     */
    protected <T> T execute(String type, ResponseHandler<T> responseHandler, String sparql) {
        log.trace("Running SPARQL: {}", sparql);
        long startQuery = System.currentTimeMillis();
        // TODO we might want to look into Blazegraph's incremental update
        // reporting.....
        final ContentResponse response;
        try {
            response = retryer.call(()
                -> makeRequest(type, sparql, responseHandler.acceptHeader()).send());

            if (response.getStatus() != HttpStatus.OK_200) {
                throw new ContainedException("Non-200 response from triple store:  " + response
                                + " body=\n" + responseBodyAsString(response));
            }

            log.debug("Completed in {} ms", System.currentTimeMillis() - startQuery);
            return responseHandler.parse(response);
        } catch (ExecutionException | RetryException | IOException e) {
            throw new FatalException("Error updating triple store", e);
        }
    }

    /**
     * Fetch the body of the response as a string.
     *
     * @throws IOException if there is an error reading the response
     */
    protected String responseBodyAsString(ContentResponse response) throws IOException {
        return response.getContentAsString();
    }

    /**
     * Run a query that returns just a date in the "date" binding and return its
     * result.
     */
    private Date dateFromQuery(String query) {
        TupleQueryResult result = query(query);
        try {
            if (!result.hasNext()) {
                return null;
            }
            Binding maxLastUpdate = result.next().getBinding("date");
            if (maxLastUpdate == null) {
                return null;
            }
            XMLGregorianCalendar xmlCalendar = ((Literal) maxLastUpdate.getValue()).calendarValue();
            /*
             * We convert rather blindly to a GregorianCalendar because we're
             * reasonably sure all the right data is present.
             */
            GregorianCalendar calendar = xmlCalendar.toGregorianCalendar();
            return calendar.getTime();
        } catch (QueryEvaluationException e) {
            throw new FatalException("Error evaluating query", e);
        }
    }

    /**
     * Passed to execute to setup the accept header and parse the response. Its
     * super ultra mega important to parse the response in execute because
     * execute manages closing the http response object. If execute return the
     * input stream after closing the response then everything would
     * <strong>mostly</strong> work but things would blow up with strange socket
     * closed errors.
     *
     * @param <T> the type of response parsed
     */
    private interface ResponseHandler<T> {
        /**
         * The contents of the accept header sent to the rdf repository.
         */
        String acceptHeader();

        /**
         * Parse the response.
         *
         * @throws IOException if there is an error reading the response
         */
        T parse(ContentResponse entity) throws IOException;
    }

    /**
     * Count and log the number of updates.
     */
    protected static final ResponseHandler<Integer> UPDATE_COUNT_RESPONSE = new UpdateCountResponse();
    /**
     * Parse the response from a regular query into a TupleQueryResult.
     */
    protected static final ResponseHandler<TupleQueryResult> TUPLE_QUERY_RESPONSE = new TupleQueryResponse();
    /**
     * Parse the response from an ask query into a boolean.
     */
    protected static final ResponseHandler<Boolean> ASK_QUERY_RESPONSE = new AskQueryResponse();

    /**
     * Attempts to log update response information but very likely only works
     * for Blazegraph.
     */
    protected static class UpdateCountResponse implements ResponseHandler<Integer> {
        /**
         * The pattern for the response for an update.
         */
        private static final Pattern ELAPSED_LINE = Pattern.compile("><p>totalElapsed=[^ ]+ elapsed=([^<]+)</p");
        /**
         * The pattern for the response for an update, with extended times for clauses.
         */
        private static final Pattern ELAPSED_LINE_CLAUSES =
                Pattern.compile("><p>totalElapsed=([^ ]+) elapsed=([^ ]+) whereClause=([^ ]+) deleteClause=([^ ]+) insertClause=([^ <]+)</p");
        /**
         * The pattern for the response for an update, with extended times for clauses and flush.
         */
        private static final Pattern ELAPSED_LINE_FLUSH =
                Pattern.compile("><p>totalElapsed=([^ ]+) elapsed=([^ ]+) connFlush=([^ ]+) " +
                        "batchResolve=([^ ]+) whereClause=([^ ]+) deleteClause=([^ ]+) insertClause=([^ <]+)</p");
        /**
         * The pattern for the response for a commit.
         */
        private static final Pattern COMMIT_LINE = Pattern
                .compile("><hr><p>COMMIT: totalElapsed=([^ ]+) commitTime=[^ ]+ mutationCount=([^<]+)</p");
        /**
         * The pattern for the response from a bulk update.
         */
        private static final Pattern BULK_UPDATE_LINE = Pattern
                .compile("<\\?xml version=\"1.0\"\\?><data modified=\"(\\d+)\" milliseconds=\"(\\d+)\"/>");

        @Override
        public String acceptHeader() {
            return null;
        }

        @Override
        @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "more readable with 2 calls")
        public Integer parse(ContentResponse entity) throws IOException {
            Integer mutationCount = null;
            for (String line : entity.getContentAsString().split("\\r?\\n")) {
                Matcher m;
                m = ELAPSED_LINE_FLUSH.matcher(line);
                if (m.matches()) {
                    log.debug("total = {} elapsed = {} flush = {} batch = {} where = {} delete = {} insert = {}",
                            m.group(1), m.group(2), m.group(3), m.group(4),
                            m.group(5), m.group(6), m.group(7));
                    continue;
                }
                m = ELAPSED_LINE_CLAUSES.matcher(line);
                if (m.matches()) {
                    log.debug("total = {} elapsed = {} where = {} delete = {} insert = {}",
                            m.group(1), m.group(2), m.group(3), m.group(4),
                            m.group(5));
                    continue;
                }
                m = ELAPSED_LINE.matcher(line);
                if (m.matches()) {
                    log.debug("elapsed = {}", m.group(1));
                    continue;
                }
                m = COMMIT_LINE.matcher(line);
                if (m.matches()) {
                    log.debug("total = {} mutation count = {} ", m.group(1),
                            m.group(2));
                    mutationCount = Integer.valueOf(m.group(2));
                    continue;
                }
                m = BULK_UPDATE_LINE.matcher(line);
                if (m.matches()) {
                    log.debug("bulk updated {} items in {} millis", m.group(1),
                            m.group(2));
                    mutationCount = Integer.valueOf(m.group(1));
                    continue;
                }
            }
            if (mutationCount == null) {
                throw new IOException("Couldn't find the mutation count!");
            }
            return mutationCount;
        }
    }

    /**
     * Parses responses to regular queries into TupleQueryResults.
     */
    private static class TupleQueryResponse implements ResponseHandler<TupleQueryResult> {
        @Override
        public String acceptHeader() {
            return "application/x-binary-rdf-results-table";
        }

        @Override
        public TupleQueryResult parse(ContentResponse entity) throws IOException {
            BinaryQueryResultParser p = new BinaryQueryResultParser();
            TupleQueryResultBuilder collector = new TupleQueryResultBuilder();
            p.setQueryResultHandler(collector);
            try {
                p.parseQueryResult(new ByteArrayInputStream(entity.getContent()));
            } catch (QueryResultParseException | QueryResultHandlerException | IllegalStateException e) {
                throw new RuntimeException("Error parsing query", e);
            }
            return collector.getQueryResult();
        }
    }

    /**
     * Parses responses to ask queries into booleans.
     */
    private static class AskQueryResponse implements ResponseHandler<Boolean> {
        @Override
        public String acceptHeader() {
            return "application/json";
        }

        @Override
        public Boolean parse(ContentResponse entity) throws IOException {
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                return mapper.readValue(entity.getContentAsString(), Resp.class).aBoolean;

            } catch (JsonParseException | JsonMappingException e) {
                throw new IOException("Error parsing response", e);
            }
        }

        public static class Resp {
            private final Boolean aBoolean;

            @JsonCreator
            Resp(@JsonProperty("boolean") Boolean aBoolean) {
                this.aBoolean = aBoolean;
            }
        }
    }

}
