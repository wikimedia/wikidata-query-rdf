package org.wikidata.query.rdf.tool.wikibase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.HttpClientUtils;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.ContainedException;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.NormalizingRdfHandler;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wraps Wikibase api.
 */
// TODO fan out complexity
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class WikibaseRepository {
    private static final Logger log = LoggerFactory.getLogger(WikibaseRepository.class);

    /**
     * How many retries allowed on error.
     */
    private static final int RETRIES = 3;

    /**
     * Retry interval, in ms.
     */
    private static final int RETRY_INTERVAL = 500;

    /**
     * HTTP client for wikibase.
     */
    private final CloseableHttpClient client = HttpClients.custom()
            .setMaxConnPerRoute(100).setMaxConnTotal(100)
            .setRetryHandler(getRetryHandler(RETRIES))
            .setServiceUnavailableRetryStrategy(getRetryStrategy(RETRIES, RETRY_INTERVAL))
            .setUserAgent("Wikidata Query Service Updater")
            .build();

    /**
     * Builds uris to get stuff from wikibase.
     */
    private final Uris uris;

    public WikibaseRepository(String scheme, String host) {
        uris = new Uris(scheme, host);
    }

    public WikibaseRepository(String scheme, String host, int port) {
        uris = new Uris(scheme, host, port);
    }

    public WikibaseRepository(String scheme, String host, int port, long[] entityNamespaces) {
        uris = new Uris(scheme, host, port, entityNamespaces);
    }

    /**
     * Return retry strategy for "service unavailable".
     * This one handles 503 and 429 by retrying it after a fixed period.
     * TODO: 429 may contain header that we may want to use for retrying?
     * @param max Maximum number of retries.
     * @param interval Interval between retries, ms.
     * @see DefaultServiceUnavailableRetryStrategy
     * @return
     */
    private static ServiceUnavailableRetryStrategy getRetryStrategy(final int max, final int interval) {
        // This is the same as DefaultServiceUnavailableRetryStrategy but also handles 429
        return new ServiceUnavailableRetryStrategy() {
            @Override
            public boolean retryRequest(final HttpResponse response, final int executionCount, final HttpContext context) {
                return executionCount <= max &&
                    (response.getStatusLine().getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE ||
                    response.getStatusLine().getStatusCode() == 429);
            }

            @Override
            public long getRetryInterval() {
                return interval;
            }
        };
    }

    /**
     * Create retry handler.
     * Note: this is for retrying I/O exceptions.
     * @param max Maximum retries number.
     * @return
     */
    private static HttpRequestRetryHandler getRetryHandler(final int max) {
        HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception, int executionCount,
                    HttpContext context) {
                log.debug("Exception: {} in attempt {}", exception, executionCount);
                if (executionCount >= max) {
                    // Do not retry if over max retry count
                    return false;
                }
                if (exception instanceof InterruptedIOException) {
                    // Timeout
                    return true;
                }
                if (exception instanceof UnknownHostException) {
                    // Unknown host
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) {
                    // Connection refused
                    return true;
                }
                if (exception instanceof SSLException) {
                    // SSL handshake exception
                    return false;
                }

                HttpClientContext clientContext = HttpClientContext.adapt(context);
                HttpRequest request = clientContext.getRequest();
                boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
                if (idempotent) {
                    // Retry if the request is considered idempotent
                    return true;
                }

                return false;
            }

        };
        return myRetryHandler;
    }

    /**
     * Fetch recent changes starting from nextStartTime or continuing from
     * lastContinue depending on the contents of lastContinue way to use
     * MediaWiki. See RecentChangesPoller for how to poll these. Or just use it.
     *
     * @param nextStartTime if lastContinue is null then this is the start time
     *            of the query
     * @param batchSize the number of recent changes to fetch
     * @return result of query
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public JSONObject fetchRecentChangesByTime(Date nextStartTime, int batchSize) throws RetryableException {
        return fetchRecentChanges(nextStartTime, null, batchSize);
    }

    /**
     * Fetch recent changes starting from nextStartTime or continuing from
     * lastContinue depending on the contents of lastContinue way to use
     * MediaWiki. See RecentChangesPoller for how to poll these. Or just use it.
     *
     * @param nextStartTime if lastContinue is null then this is the start time
     *            of the query
     * @param batchSize the number of recent changes to fetch
     * @param lastContinue Continuation object from last batch, or null.
     * @return result of query
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public JSONObject fetchRecentChanges(Date nextStartTime, JSONObject lastContinue, int batchSize)
            throws RetryableException {
        URI uri = uris.recentChanges(nextStartTime, lastContinue, batchSize);
        log.debug("Polling for changes from {}", uri);
        try {
            return checkApi(getJson(new HttpGet(uri)));
        } catch (UnknownHostException | SocketException e) {
            // We want to bail on this, since it happens to be sticky for some reason
            throw new RuntimeException(e);
        } catch (IOException | ParseException e) {
            throw new RetryableException("Error fetching recent changes", e);
        }
    }

    /**
     * Fetch the RDF for some entity.
     *
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public Collection<Statement> fetchRdfForEntity(String entityId) throws RetryableException {
        // TODO handle ?flavor=dump or whatever parameters we need
        URI uri = uris.rdf(entityId);
        long start = System.currentTimeMillis();
        log.debug("Fetching rdf from {}", uri);
        RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
        StatementCollector collector = new StatementCollector();
        parser.setRDFHandler(new NormalizingRdfHandler(collector));
        HttpGet request = new HttpGet(uri);
        HttpClientUtils.ignoreCookies(request);
        try {
            try (CloseableHttpResponse response = client.execute(request)) {
                if (response.getStatusLine().getStatusCode() == 404) {
                    // A delete/nonexistent page
                    return Collections.emptyList();
                }
                if (response.getStatusLine().getStatusCode() >= 300) {
                    throw new ContainedException("Unexpected status code fetching RDF for " + uri + ":  "
                            + response.getStatusLine().getStatusCode());
                }
                parser.parse(new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8), uri.toString());
            }
        } catch (UnknownHostException | SocketException | SSLHandshakeException e) {
            // We want to bail on this, since it happens to be sticky for some reason
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RetryableException("Error fetching RDF for " + uri, e);
        } catch (RDFParseException | RDFHandlerException e) {
            throw new ContainedException("RDF parsing error for " + uri, e);
        }
        log.debug("Done in {} ms", System.currentTimeMillis() - start);
        return collector.getStatements();
    }

    /**
     * Get the first id with the provided label in the provided language.
     *
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public String firstEntityIdForLabelStartingWith(String label, String language, String type)
            throws RetryableException {
        URI uri = uris.searchForLabel(label, language, type);
        log.debug("Searching for entity using {}", uri);
        try {
            JSONObject result = checkApi(getJson(new HttpGet(uri)));
            JSONArray resultList = (JSONArray) result.get("search");
            if (resultList.isEmpty()) {
                return null;
            }
            result = (JSONObject) resultList.get(0);
            return result.get("id").toString();
        } catch (IOException | ParseException e) {
            throw new RetryableException("Error searching for page", e);
        }
    }

    /**
     * Edits or creates a page by setting a label. Used for testing.
     *
     * @param entityId id of the entity - if null then the entity will be
     *            created new
     * @param type type of entity to create or edit
     * @param label label of the page to create
     * @param language language of the label to add
     * @return the entityId
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    @SuppressWarnings("unchecked")
    public String setLabel(String entityId, String type, String label, String language) throws RetryableException {
        JSONObject data = new JSONObject();
        JSONObject labels = new JSONObject();
        data.put("labels", labels);
        JSONObject labelObject = new JSONObject();
        labels.put("en", labelObject);
        labelObject.put("language", language);
        labelObject.put("value", label + System.currentTimeMillis());
        if (type.equals("property")) {
            // A data type is required for properties so lets just pick one
            data.put("datatype", "string");
        }
        URI uri = uris.edit(entityId, type, data.toJSONString());
        log.debug("Editing entity using {}", uri);
        try {
            JSONObject result = checkApi(getJson(postWithToken(uri)));
            return ((JSONObject) result.get("entity")).get("id").toString();
        } catch (IOException | ParseException e) {
            throw new RetryableException("Error adding page", e);
        }
    }

    /**
     * Delete entity from repository.
     * @param entityId
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    public void delete(String entityId) throws RetryableException {
        URI uri = uris.delete(entityId);
        log.debug("Deleting entity {} using {}", entityId, uri);
        try {
            JSONObject result = checkApi(getJson(postWithToken(uri)));
            log.debug("Deleted: {}", result);
        } catch (IOException | ParseException e) {
            throw new RetryableException("Error deleting page", e);
        }
    }

    /**
     * Post with a csrf token.
     *
     * @throws IOException if its thrown while communicating with wikibase
     * @throws ParseException if wikibase's response can't be parsed
     */
    private HttpPost postWithToken(URI uri) throws IOException, ParseException {
        HttpPost request = new HttpPost(uri);
        List<NameValuePair> entity = new ArrayList<>();
        entity.add(new BasicNameValuePair("token", csrfToken()));
        request.setEntity(new UrlEncodedFormEntity(entity, Consts.UTF_8));
        return request;
    }

    /**
     * Fetch a csrf token.
     *
     * @throws IOException if its thrown while communicating with wikibase
     * @throws ParseException if wikibase's response can't be parsed
     */
    private String csrfToken() throws IOException, ParseException {
        URI uri = uris.csrfToken();
        log.debug("Fetching csrf token from {}", uri);
        return ((JSONObject) ((JSONObject) getJson(new HttpGet(uri)).get("query")).get("tokens")).get("csrftoken")
                .toString();
    }

    /**
     * Perform an HTTP request and return the JSON in the response body.
     *
     * @param request request to perform
     * @return json response
     * @throws IOException if there is an error parsing the json or if one is
     *             thrown receiving the data
     * @throws ParseException the json was malformed and couldn't be parsed
     */
    private JSONObject getJson(HttpRequestBase request) throws IOException, ParseException {
        HttpClientUtils.ignoreCookies(request);
        try (CloseableHttpResponse response = client.execute(request)) {
            return (JSONObject) new JSONParser().parse(new InputStreamReader(response.getEntity().getContent(),
                    Charsets.UTF_8));
        }
    }

    /**
     * Check that a response from the Mediawiki api isn't an error.
     *
     * @param response the response
     * @return the response again
     * @throws RetryableException thrown if there is an error communicating with
     *             wikibase
     */
    private JSONObject checkApi(JSONObject response) throws RetryableException {
        Object error = response.get("error");
        if (error != null) {
            throw new RetryableException("Error result from Mediawiki:  " + error);
        }
        return response;
    }

    /**
     * Check that a namespace is valid wikibase entity namespace.
     *
     * @param namespace the namespace index
     * @return
     */
    public boolean isEntityNamespace(long namespace) {
        return ArrayUtils.contains(uris.getEntityNamespaces(), namespace);
    }

    /**
     * Check if the entity ID is a valid entity ID.
     * @param name
     * @return
     */
    public boolean isValidEntity(String name) {
        return name.matches("^[A-Za-z0-9:]+$");
    }

    /**
     * URIs used for accessing wikibase.
     */
    public static class Uris {
        /**
         * Uri scheme for wikibase.
         */
        private final String scheme;
        /**
         * Host for wikibase.
         */
        private final String host;
        /**
         * Port to connect to.
         */
        private final int port;
        /**
         * Item and Property namespaces.
         */
        private long[] entityNamespaces = {0, 120};

        public Uris(String scheme, String host) {
            this.scheme = scheme;
            this.host = host;
            this.port = 0;
        }

        public Uris(String scheme, String host, int port) {
            this.scheme = scheme;
            this.host = host;
            this.port = port;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "minor enough")
        public Uris(String scheme, String host, int port, long[] entityNamespaces) {
            this.scheme = scheme;
            this.host = host;
            this.port = port;
            this.entityNamespaces = entityNamespaces;
        }

        /**
         * Uri to get the recent changes.
         *
         * @param startTime the first date to poll from - usually if
         *            continueObject isn't null this is ignored by wikibase
         * @param continueObject Continue object from the last request
         * @param batchSize maximum number of results we want back from wikibase
         */
        public URI recentChanges(Date startTime, JSONObject continueObject, int batchSize) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "query");
            builder.addParameter("list", "recentchanges");
            builder.addParameter("rcdir", "newer");
            builder.addParameter("rcprop", "title|ids|timestamp");
            builder.addParameter("rcnamespace", getEntityNamespacesString("|"));
            builder.addParameter("rclimit", Integer.toString(batchSize));
            if (continueObject == null) {
                builder.addParameter("continue", "");
                builder.addParameter("rcstart", outputDateFormat().format(startTime));
            } else {
                builder.addParameter("continue", continueObject.get("continue").toString());
                builder.addParameter("rccontinue", continueObject.get("rccontinue").toString());
            }
            return build(builder);
        }

        /**
         * Uri to get the rdf for an entity.
         */
        public URI rdf(String entityId) {
            URIBuilder builder = builder();
            /*
             * Note that we could use /entity/%s.ttl for production Wikidata but
             * not all Wikibase instances have the rewrite rule set up. I'm
             * looking at you test.
             */
            builder.setPath(String.format(Locale.ROOT, "/wiki/Special:EntityData/%s.ttl", entityId));
            // Cache is not our friend, try to work around it
            builder.addParameter("nocache", Long.toString(new Date().getTime()));
            builder.addParameter("flavor", "dump");
            return build(builder);
        }

        /**
         * Uri to fetch a csrf token.
         */
        public URI csrfToken() {
            URIBuilder builder = apiBuilder();
            builder.setParameter("action", "query");
            builder.setParameter("meta", "tokens");
            builder.setParameter("continue", "");
            return build(builder);
        }

        /**
         * Uri to search for a label in a language.
         *
         * @param label the label to search
         * @param language the language to search
         * @param type the type of the entity
         */
        public URI searchForLabel(String label, String language, String type) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "wbsearchentities");
            builder.addParameter("search", label);
            builder.addParameter("language", language);
            builder.addParameter("type", type);
            return build(builder);
        }

        /**
         * Uri to which you can post to edit an entity.
         *
         * @param entityId the id to edit
         * @param newType the type of the entity to create. Ignored if entityId
         *            is not null.
         * @param data data to add to the entity
         */
        public URI edit(String entityId, String newType, String data) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "wbeditentity");
            if (entityId != null) {
                builder.addParameter("id", entityId);
            } else {
                builder.addParameter("new", newType);
            }
            builder.addParameter("data", data);
            return build(builder);
        }

        /**
         * Uri for deleting an entity.
         * @param entityId Entity ID to delete
         */
        public URI delete(String entityId) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "delete");
            builder.addParameter("title", entityId);
            return build(builder);
        }

        /**
         * Build a URIBuilder for wikibase apis.
         */
        private URIBuilder apiBuilder() {
            URIBuilder builder = builder();
            builder.setPath("/w/api.php");
            builder.addParameter("format", "json");
            return builder;
        }

        /**
         * Build a URIBuilder for wikibase requests.
         */
        public URIBuilder builder() {
            URIBuilder builder = new URIBuilder();
            builder.setHost(host);
            builder.setScheme(scheme);
            if (port != 0) {
                builder.setPort(port);
            }
            return builder;
        }

        /**
         * Build a URI from an URI builder, throwing a FatalException if it
         * fails.
         */
        private URI build(URIBuilder builder) {
            try {
                return builder.build();
            } catch (URISyntaxException e) {
                throw new FatalException("Unable to build url!?", e);
            }
        }

        /**
         * The wikibase host.
         */
        public String getHost() {
            return host;
        }

        /**
         * The uri scheme for the wikibase instance.
         */
        public String getScheme() {
            return scheme;
        }

        /**
         * The wikibase entity namespace indexes.
         */
        private long[] getEntityNamespaces() {
            return entityNamespaces;
        }

        /**
         * The wikibase entity namespace indexes joined with a delimiter.
         */
        private String getEntityNamespacesString(String delimiter) {
            return Longs.join(delimiter, entityNamespaces);
        }

    }

    /**
     * Create a new DateFormat object that parses from and formats to the date
     * in the format that wikibase wants as input.
     */
    public static DateFormat outputDateFormat() {
        return utc(new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT));
    }

    /**
     * Create a new DateFormat object that parses from and formats to the date
     * in the format that wikibase returns.
     */
    public static DateFormat inputDateFormat() {
        return utc(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT));
    }

    /**
     * Convert a DateFormat to always output in utc.
     */
    private static DateFormat utc(DateFormat df) {
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df;
    }

    /**
     * Create JSON change description for continuing.
     * @param lastChange
     * @return Change description that can be used to continue from the next change.
     */
    @SuppressWarnings("unchecked")
    public JSONObject getContinueObject(Change lastChange) {
        JSONObject nextContinue = new JSONObject();
        nextContinue.put("rccontinue", outputDateFormat().format(lastChange.timestamp()) + "|" + (lastChange.rcid() + 1));
        nextContinue.put("continue", "-||");
        return nextContinue;
    }

    /**
     * Extract timestamp from continue JSON object.
     * @param nextContinue
     * @return Timestamp as date
     * @throws java.text.ParseException When data is in is wrong format
     */
    public Change getChangeFromContinue(Map<String, Object> nextContinue) throws java.text.ParseException {
        if (nextContinue == null) {
            return null;
        }
        final String rccontinue = (String)nextContinue.get("rccontinue");
        final String[] parts = rccontinue.split("\\|");
        return new Change("DUMMY", -1, outputDateFormat().parse(parts[0]), Long.parseLong(parts[1]));
    }

}
