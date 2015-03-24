package org.wikidata.query.rdf.tool.wikibase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
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

import com.google.common.base.Charsets;

/**
 * Wraps Wikibase api.
 */
public class WikibaseRepository {
    private static final Logger log = LoggerFactory.getLogger(WikibaseRepository.class);
    private final CloseableHttpClient client = HttpClientBuilder.create().build();
    private final Uris uris;

    public WikibaseRepository(String scheme, String host) {
        uris = new Uris(scheme, host);
    }

    /**
     * Fetch recent changes starting from nextStartTime or continuing from
     * lastContinue depending on the contents of lastContinue way to use
     * MediaWiki.
     *
     * <p>
     * You can fetch the continuation information like so:
     * <p>
     * <code>
     * lastContinue = (JSONObject) recentChanges.get("continue");
     * </code>
     * <p>
     * and you can fetch the changes like so:
     * <p>
     * <code>
     * JSONArray result = (JSONArray) ((JSONObject) recentChanges.get("query")).get("recentchanges");
     * </code>
     *
     * @param nextStartTime if lastContinue is null then this is the start time
     *            of the query
     * @param lastContinue continuation point if not null
     * @return result of query
     * @throws IOException if there are io or parse errors
     */
    public JSONObject fetchRecentChanges(long nextStartTime, JSONObject lastContinue) throws IOException {
        URI uri = uris.recentChanges(nextStartTime, lastContinue);
        log.debug("Polling for changes from {}", uri);
        return checkApi(getJson(new HttpGet(uri)));
    }

    /**
     * Fetch the RDF for some entity.
     */
    public Collection<Statement> fetchRdfForEntity(String entityId) throws IOException {
        URI uri = uris.rdf(entityId);
        log.debug("Fetching rdf from {}", uri);
        RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
        StatementCollector collector = new StatementCollector();
        parser.setRDFHandler(collector);
        try (CloseableHttpResponse response = client.execute(new HttpGet(uri))) {
            parser.parse(new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8), uri.toString());
        } catch (RDFParseException e) {
            throw new IOException("RDF parsing error for " + uri, e);
        } catch (RDFHandlerException e) {
            throw new IOException("RDF handling error for " + uri, e);
        }
        return collector.getStatements();
    }

    /**
     * Creates a page with label in English. Used for testing.
     *
     * @param label English label of the page to find or create
     * @return the entityId
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public String addPage(String label) throws IOException {
        JSONObject data = new JSONObject();
        JSONObject labels = new JSONObject();
        data.put("labels", labels);
        JSONObject en = new JSONObject();
        labels.put("en", en);
        en.put("language", "en");
        en.put("value", label);
        URI uri = uris.edit(null, data.toJSONString());
        log.debug("Creating entity using {}", uri);
        JSONObject result = checkApi(getJson(postWithToken(uri)));
        return ((JSONObject) result.get("entity")).get("id").toString();
    }

    private HttpPost postWithToken(URI uri) throws IOException {
        HttpPost request = new HttpPost(uri);
        List<NameValuePair> entity = new ArrayList<>();
        entity.add(new BasicNameValuePair("token", csrfToken()));
        request.setEntity(new UrlEncodedFormEntity(entity, Consts.UTF_8));
        return request;
    }

    private String csrfToken() throws IOException {
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
     */
    private JSONObject getJson(HttpUriRequest request) throws IOException {
        try (CloseableHttpResponse response = client.execute(request)) {
            try {
                return (JSONObject) new JSONParser().parse(new InputStreamReader(response.getEntity().getContent(),
                        Charsets.UTF_8));
            } catch (ParseException e) {
                throw new IOException("Parse error from api", e);
            }
        }
    }

    /**
     * Check that a response from the Mediawiki api isn't an error.
     *
     * @param response the response
     * @return the response again
     */
    private JSONObject checkApi(JSONObject response) {
        Object error = response.get("error");
        if (error != null) {
            throw new RuntimeException("Error result from Mediawiki:  " + error);
        }
        return response;
    }

    /**
     * URIs used for accessing Wikibase.
     */
    private class Uris {
        private final String scheme;
        private final String host;

        public Uris(String scheme, String host) {
            this.scheme = scheme;
            this.host = host;
        }

        public URI recentChanges(long nextStartTime, JSONObject lastContinue) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "query");
            builder.addParameter("list", "recentchanges");
            builder.addParameter("rcdir", "newer");
            builder.addParameter("rcprop", "title|ids|timestamp");
            if (lastContinue == null) {
                builder.addParameter("continue", "");
                builder.addParameter("rcstart", outputDateFormat().format(nextStartTime));
            } else {
                builder.addParameter("continue", lastContinue.get("continue").toString());
                builder.addParameter("rccontinue", lastContinue.get("rccontinue").toString());
            }
            return build(builder);
        }

        public URI rdf(String title) {
            URIBuilder builder = builder();
            /*
             * Note that we could use /entity/%s.ttl for production Wikidata but
             * not all Wikibase instances have the rewrite rule set up.
             */
            builder.setPath(String.format(Locale.ROOT, "/wiki/Special:EntityData/%s.ttl", title));
            builder.addParameter("nocache", "");
            return build(builder);
        }

        public URI csrfToken() {
            URIBuilder builder = apiBuilder();
            builder.setParameter("action", "query");
            builder.setParameter("meta", "tokens");
            builder.setParameter("continue", "");
            return build(builder);
        }

        public URI edit(String entityId, String data) {
            URIBuilder builder = apiBuilder();
            builder.addParameter("action", "wbeditentity");
            if (entityId != null) {
                builder.addParameter("id", entityId);
            } else {
                builder.addParameter("new", "item");
            }
            builder.addParameter("data", data);
            return build(builder);
        }

        private URIBuilder apiBuilder() {
            URIBuilder builder = builder();
            builder.setPath("/w/api.php");
            builder.addParameter("format", "json");
            return builder;
        }

        private URIBuilder builder() {
            URIBuilder builder = new URIBuilder();
            builder.setHost(host);
            builder.setScheme(scheme);
            return builder;
        }

        private URI build(URIBuilder builder) {
            try {
                return builder.build();
            } catch (URISyntaxException e) {
                throw new RuntimeException("Unable to build url!?", e);
            }
        }
    }

    public static DateFormat outputDateFormat() {
        return utc(new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT));
    }

    public static DateFormat inputDateFormat() {
        return utc(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT));
    }

    private static DateFormat utc(DateFormat df) {
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df;
    }
}
