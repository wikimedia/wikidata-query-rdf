package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.io.Resources.getResource;
import static org.wikidata.query.rdf.tool.FilteredStatements.filtered;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.query.Binding;
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
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

/**
 * Wrapper for communicating with the RDF repository.
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class RdfRepository {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);
    /**
     * UTC timezone.
     */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    /**
     * Http connection pool for the rdf repository.
     */
    private final CloseableHttpClient client = HttpClients.custom().setMaxConnPerRoute(100).setMaxConnTotal(100)
            .build();
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
     * Sparql for a portion of the update.
     */
    private final String syncBody;
    /**
     * Sparql for a portion of the update.
     */
    private final String getValues;
    /**
     * Sparql for a portion of the update.
     */
    private final String getRefs;
    /**
     * Sparql for a portion of the update.
     */
    private final String cleanUnused;
    /**
     * Sparql to sync the left off time.
     */
    private final String updateLeftOffTimeBody;

    /**
     * How many times we retry a failed HTTP call.
     */
    private int maxRetries = 5;
    /**
     * How long to delay after failing first HTTP call, in milliseconds.
     * Next retries would be slower by 2x, 3x, 4x etc. until maxRetries is exhausted.
     */
    private int delay = 1000;

    /**
     * Allow subclass access to the HTTP client.
     */
    protected CloseableHttpClient client() {
        return client;
    }

    public RdfRepository(URI uri, WikibaseUris uris) {
        this.uri = uri;
        this.uris = uris;
        syncBody = loadBody("sync");
        updateLeftOffTimeBody = loadBody("updateLeftOffTime");
        getValues = loadBody("GetValues");
        getRefs = loadBody("GetRefs");
        cleanUnused = loadBody("CleanUnused");
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
     * @return
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
            throw new FatalException("Can't load " + url);
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
     * Get list of value subjects connected to entity. The connection is either
     * via statement or via reference or via qualifier.
     *
     * @param entityId
     * @return Set of value subjects
     */
    public Set<String> getValues(String entityId) {
        UpdateBuilder b = new UpdateBuilder(getValues);
        b.bindUri("entity:id", uris.entity() + entityId);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToSet(query(b.toString()), "s");
    }

    /**
     * Get list of reference subjects connected to entity.
     *
     * @param entityId
     * @return Set of references
     */
    public Set<String> getRefs(String entityId) {
        UpdateBuilder b = new UpdateBuilder(getRefs);
        b.bindUri("entity:id", uris.entity() + entityId);
        b.bind("uris.statement", uris.statement());
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);

        return resultToSet(query(b.toString()), "s");
    }

    /**
     * Clean subjects if they are not used anymore. The candidate values do not
     * have to be actually unused - the cleanup query will figure out which are
     * unused and delete only those.
     *
     * @param valueList List of potential candidates for cleanup.
     */
    public void cleanUnused(Collection<String> valueList) {
        if (valueList.isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();
        UpdateBuilder b = new UpdateBuilder(cleanUnused);
        b.bindUris("values", valueList);
        int modified = execute("update", UPDATE_COUNT_RESPONSE, b.toString());
        log.debug("Cleanup {} millis and modified {} statements", System.currentTimeMillis() - start, modified);
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
     * @return the number of statements modified
     */
    public int sync(String entityId, Collection<Statement> statements) {
        // TODO this is becoming a mess too
        log.debug("Updating data for {}", entityId);
        UpdateBuilder b = new UpdateBuilder(syncBody);
        b.bindUri("entity:id", uris.entity() + entityId);
        b.bindUri("schema:about", SchemaDotOrg.ABOUT);
        b.bindUri("prov:wasDerivedFrom", Provenance.WAS_DERIVED_FROM);
        b.bind("uris.value", uris.value());
        b.bind("uris.statement", uris.statement());
        b.bindStatements("insertStatements", statements);
        b.bindValues("valueStatements", statements);
        b.bindValues("entityStatements", filtered(statements).withSubject(uris.entity() + entityId));

        long start = System.currentTimeMillis();
        int modified = execute("update", UPDATE_COUNT_RESPONSE, b.toString());
        log.debug("Updating {} took {} millis and modified {} statements", entityId,
                System.currentTimeMillis() - start, modified);
        return modified;
    }

    /**
     * Does the triple store have this revision or better.
     */
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
     * Execute some raw SPARQL.
     *
     * @param type name of the parameter in which to send sparql
     * @return results string from the server
     */
    protected <T> T execute(String type, ResponseHandler<T> responseHandler, String sparql) {
        HttpPost post = new HttpPost(uri);
        post.setHeader(new BasicHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"));
        // Note that Blazegraph totally ignores the Accept header for SPARQL
        // updates like this so the response is just html....
        if (responseHandler.acceptHeader() != null) {
            post.setHeader(new BasicHeader("Accept", responseHandler.acceptHeader()));
        }
        log.debug("Running SPARQL: {}", sparql);
        // TODO we might want to look into Blazegraph's incremental update
        // reporting.....
        List<NameValuePair> entity = new ArrayList<>();
        entity.add(new BasicNameValuePair(type, sparql));
        post.setEntity(new UrlEncodedFormEntity(entity, Consts.UTF_8));
        int retries = 0;
        while (true) {
            try {
                try (CloseableHttpResponse response = client.execute(post)) {
                    if (response.getStatusLine().getStatusCode() != 200) {
                        throw new ContainedException("Non-200 response from triple store:  " + response + " body=\n"
                                + responseBodyAsString(response));
                    }
                    return responseHandler.parse(response.getEntity());
                }
            } catch (IOException e) {
                if (retries < maxRetries) {
                    // Increasing delay, with random 10% variation so threads won't all get restarts
                    // at the same time.
                    int retryIn = (int)Math.ceil(delay * (retries + 1) * (1 + Math.random() * 0.1));
                    log.info("HTTP request failed: {}, retrying in {} ms", e, retryIn);
                    retries++;
                    try {
                        Thread.sleep(retryIn);
                    } catch (InterruptedException e1) {
                        throw new FatalException("Interrupted", e);
                    }
                    continue;
                }
                throw new FatalException("Error updating triple store", e);
            }
        }

    }

    /**
     * Fetch the body of the response as a string.
     *
     * @throws IOException if there is an error reading the response
     */
    protected String responseBodyAsString(CloseableHttpResponse response) throws IOException {
        return CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
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
        T parse(HttpEntity entity) throws IOException;
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
         * The pattern for the response for a commit.
         */
        private static final Pattern COMMIT_LINE = Pattern
                .compile("><hr><p>COMMIT: totalElapsed=[^ ]+ commitTime=[^ ]+ mutationCount=([^<]+)</p");
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
        public Integer parse(HttpEntity entity) throws IOException {
            Integer mutationCount = null;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), Charsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher m = ELAPSED_LINE.matcher(line);
                    if (m.matches()) {
                        log.trace("elapsed = {}", m.group(1));
                        continue;
                    }
                    m = COMMIT_LINE.matcher(line);
                    if (m.matches()) {
                        log.debug("mutation count = {}", m.group(1));
                        mutationCount = Integer.valueOf(m.group(1));
                        continue;
                    }
                    m = BULK_UPDATE_LINE.matcher(line);
                    if (m.matches()) {
                        log.debug("bulk updated {} items in {} millis", m.group(1), m.group(2));
                        mutationCount = Integer.valueOf(m.group(1));
                        continue;
                    }
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
        public TupleQueryResult parse(HttpEntity entity) throws IOException {
            BinaryQueryResultParser p = new BinaryQueryResultParser();
            TupleQueryResultBuilder collector = new TupleQueryResultBuilder();
            p.setQueryResultHandler(collector);
            try {
                p.parseQueryResult(entity.getContent());
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
        public Boolean parse(HttpEntity entity) throws IOException {
            try {
                JSONObject response = (JSONObject) new JSONParser().parse(new InputStreamReader(entity.getContent(),
                        Charsets.UTF_8));
                return (Boolean) response.get("boolean");
            } catch (ParseException e) {
                throw new IOException("Error parsing response", e);
            }
        }
    }
}
