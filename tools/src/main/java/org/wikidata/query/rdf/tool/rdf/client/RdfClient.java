package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Fields;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

/**
 * Low level API to Blazegraph.
 *
 * This provides transport and the ability to execute different kind of queries.
 */
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
    private final int timeout;
    /** Retryer for fetching data from RDF store. */
    private final Retryer<ContentResponse> retryer;

    public RdfClient(HttpClient httpClient, URI uri, int timeout, Retryer<ContentResponse> retryer) {
        this.httpClient = httpClient;
        this.uri = uri;
        this.timeout = timeout;
        this.retryer = retryer;
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
                                + " body=\n" + response.getContentAsString());
            }

            log.debug("Completed in {} ms", System.currentTimeMillis() - startQuery);
            return responseHandler.parse(response);
        } catch (ExecutionException | RetryException | IOException e) {
            throw new FatalException("Error updating triple store", e);
        }
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

}
