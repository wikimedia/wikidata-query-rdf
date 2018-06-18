package org.wikidata.query.rdf.tool.rdf;

import static org.wikidata.query.rdf.tool.MapperUtils.getObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Fields;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.binary.BinaryQueryResultParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
     * Attempts to log update response information but very likely only works
     * for Blazegraph.
     */
    private static class UpdateCountResponse implements ResponseHandler<Integer> {
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

        private final ObjectMapper mapper = getObjectMapper();

        @Override
        public String acceptHeader() {
            return "application/json";
        }

        @Override
        public Boolean parse(ContentResponse entity) throws IOException {
            try {
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
