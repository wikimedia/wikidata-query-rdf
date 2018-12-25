package org.wikidata.query.rdf.tool.rdf.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.eclipse.jetty.http.HttpMethod.POST;
import static org.eclipse.jetty.http.HttpStatus.OK_200;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.util.Fields;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;

/**
 * Low level API to Blazegraph.
 *
 * This provides transport and the ability to execute different kind of queries.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity") // This class seems simple enough as it is, but refactoring proposal are welcomed!
public class RdfClient {

    private static final Logger log = LoggerFactory.getLogger(RdfClient.class);

    /** Count and log the number of updates. */
    private static final ResponseHandler<Integer> UPDATE_COUNT_RESPONSE = new UpdateCountResponse();
    /** Parse the response from a regular query into a TupleQueryResult. */
    private static final ResponseHandler<TupleQueryResult> TUPLE_QUERY_RESPONSE = new TupleQueryResponse();
    /** Parse the response from an ask query into a boolean. */
    private static final ResponseHandler<Boolean> ASK_QUERY_RESPONSE = new AskQueryResponse();

    /** Http connection pool for the rdf repository. */
    @VisibleForTesting
    public final HttpClient httpClient;
    /** URI for the wikibase rdf repository. */
    private final URI uri;
    /** Request timeout. */
    private final Duration timeout;
    /** Retryer for fetching data from RDF store. */
    private final Retryer<ContentResponse> retryer;

    /**
     * Max POST form content size.
     * Should be in sync with Jetty org.eclipse.jetty.server.Request.maxFormContentSize setting.
     * Production default is 200M, see runBlazegraph.sh file.
     * If that setting is changed, this one should change too, otherwise we get POST errors on big updates.
     * See: https://phabricator.wikimedia.org/T210235
     */
    private final long maxPostSize;

    public RdfClient(HttpClient httpClient, URI uri, Retryer<ContentResponse> retryer, Duration timeout, long maxPostSize) {
        this.httpClient = httpClient;
        this.uri = uri;
        this.timeout = timeout;
        this.retryer = retryer;
        this.maxPostSize = maxPostSize;
    }

    /**
     * Get maximum supported post size.
     * @return Max POST size, in bytes.
     */
    public long getMaxPostSize() {
        return maxPostSize;
    }

    /**
     * Execute some SPARQL which returns a results table.
     */
    public TupleQueryResult query(String sparql) {
        return execute("query", TUPLE_QUERY_RESPONSE, sparql);
    }

    /**
     * Executes an update and returns the number of changes.
     */
    public Integer update(String sparql) {
        return execute("update", UPDATE_COUNT_RESPONSE, sparql);
    }

    /**
     * Execute a SPARQL ask and parse the boolean result.
     */
    public boolean ask(String sparql) {
        return execute("query", ASK_QUERY_RESPONSE, sparql);
    }

    /**
     * Loads a uri into this rdf repository. Uses Blazegraph's update with
     * uri's feature.
     */
    @VisibleForTesting
    public Integer loadUrl(String sparql) {
        return execute("uri", UPDATE_COUNT_RESPONSE, sparql);
    }

    /**
     * Execute some raw SPARQL.
     *
     * @param type name of the parameter in which to send sparql
     * @param <T> the type into which the result is parsed
     * @return parsed results from the server
     */
    private <T> T execute(String type, ResponseHandler<T> responseHandler, String sparql) {
        log.trace("Running SPARQL: [{}] {}", sparql.length(), sparql);
        long startQuery = System.currentTimeMillis();
        // TODO we might want to look into Blazegraph's incremental update
        // reporting.....
        final ContentResponse response;
        try {
            response = retryer.call(()
                -> makeRequest(type, sparql, responseHandler.acceptHeader()).send());

            if (response.getStatus() != OK_200) {
                throw new ContainedException("Non-200 response from triple store:  " + response
                                + " body=\n" + response.getContentAsString());
            }

            log.debug("Completed in {} ms", System.currentTimeMillis() - startQuery);
            return responseHandler.parse(response);
        } catch (ExecutionException | RetryException | IOException e) {
            throw new FatalException("Error accessing triple store", e);
        }
    }

    /**
     * Create HTTP request.
     * @param type Request type
     * @param sparql SPARQL code
     * @param accept Accept header (can be null)
     * @return Request object
     */
    private Request makeRequest(@Nonnull String type, @Nonnull String sparql, @Nullable String accept) {
        Request post = httpClient.newRequest(uri);
        post.method(POST);
        if (!timeout.isNegative()) {
            post.timeout(timeout.toMillis(), MILLISECONDS);
        }
        // Note that Blazegraph totally ignores the Accept header for SPARQL
        // updates so the response is just html in that case...
        if (accept != null) {
            post.header("Accept", accept);
        }

        if (type.equals("update")) {
            // Optimization here - use direct POST with MIME type instead of URL encoding, to save some bandwidth
            // and processing time.
            post.content(new StringContentProvider("application/sparql-update", sparql, UTF_8), "application/sparql-update; charset=UTF-8");
        } else {
            final Fields fields = new Fields();
            fields.add(type, sparql);
            final FormContentProvider form = new FormContentProvider(fields, UTF_8);
            post.content(form);
        }
        return post;
    }

    /**
     * Perform a SPARQL query and return the result as a map.
     * @param query SPARQL query, should be SELECT
     * @param keyBinding Binding name to serve as key
     * @param valueBinding Binding name to serve as values
     * @return Collection of strings resulting from the query.
     */
    public ImmutableSetMultimap<String, String> selectToMap(String query, String keyBinding, String valueBinding) {
        return resultToMap(query(query), keyBinding, valueBinding);
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
     * Perform a SPARQL query and return the result as list from one column.
     * @param query SPARQL query, should be SELECT
     * @param valueBinding Binding name to serve as values
     * @param prefix If specified, remove this prefix from the result
     * @return Collection of strings resulting from the query.
     */
    public List<String> selectToList(String query, String valueBinding, String prefix) {
        return resultToList(query(query), valueBinding, prefix);
    }

    /**
     * Convert result column to a list.
     * @param result Query result
     * @param valueBinding Name of the result variable to fetch
     * @param prefix If specified, remove this prefix from the result
     * @return List of strings resulting from the query.
     */
    private List<String> resultToList(TupleQueryResult result, String valueBinding, String prefix) {
        ImmutableList.Builder<String> values = ImmutableList.builder();
        try {
            while (result.hasNext()) {
                BindingSet bindings = result.next();
                Binding value = bindings.getBinding(valueBinding);
                if (value == null) {
                    continue;
                }
                String strValue = value.getValue().stringValue();
                if (prefix != null && strValue.startsWith(prefix)) {
                    strValue = strValue.substring(prefix.length());
                }
                values.add(strValue);
            }
        } catch (QueryEvaluationException e) {
            throw new FatalException("Can't load results: " + e, e);
        }
        return values.build();
    }

}
