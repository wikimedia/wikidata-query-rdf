package org.wikidata.query.rdf.tool.rdf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;

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
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class RdfRepository {
    private static final Logger log = LoggerFactory.getLogger(RdfRepository.class);

    private final CloseableHttpClient client = HttpClients.custom().setMaxConnPerRoute(100).setMaxConnTotal(100)
            .build();
    private final URI uri;
    private final Entity entityUris;

    public RdfRepository(URI uri, Entity entityUris) {
        this.uri = uri;
        this.entityUris = entityUris;
    }

    /**
     * Synchronizes the RDF repository's representation of an entity to be
     * exactly the provided statements.
     *
     * @param entityId id of the entity to sync
     * @param statements all known statements about the entity
     */
    public void sync(String entityId, Collection<Statement> statements) {
        UpdateBuilder siteLinksBuilder = updateBuilder();
        siteLinksBuilder.delete("?s", "?p", "?o");
        siteLinksBuilder.where("?s", "schema:about", "entity:" + entityId);
        siteLinksBuilder.where("?s", "?p", "?o");
        siteLinksBuilder.where().notExists().values(statements, "?s", "?p", "?o");

        UpdateBuilder generalBuilder = updateBuilder();
        generalBuilder.delete("entity:" + entityId, "?p", "?o");
        generalBuilder.where("entity:" + entityId, "?p", "?o");
        generalBuilder.where().notExists().values(statements, "?s", "?p", "?o");

        UpdateBuilder insertBuilder = updateBuilder();
        for (Statement statement : statements) {
            insertBuilder.insert(statement.getSubject(), statement.getPredicate(), statement.getObject());
        }
        long start = System.currentTimeMillis();
        StringBuilder command = new StringBuilder();
        command.append(siteLinksBuilder).append(";\n");
        command.append(generalBuilder).append(";\n");
        command.append(insertBuilder).append(";\n");
        execute("update", IGNORE_RESPONSE, command.toString());
        log.debug("Updating {} took {} millis", entityId, System.currentTimeMillis() - start);
    }

    /**
     * Does the triple store have this revision or better.
     */
    public boolean hasRevision(String entityId, long revision) {
        // TODO building queries with strings sucks because escaping....
        StringBuilder prefixes = new StringBuilder();
        prefixes.append("PREFIX schema: <").append(SchemaDotOrg.NAMESPACE).append(">\n");
        prefixes.append("PREFIX entity: <").append(entityUris.namespace()).append(">\n");
        return ask(String.format(Locale.ROOT, "%sASK {\n  entity:%s schema:version ?v .\n  FILTER (?v >= %s)\n}",
                prefixes, entityId, revision));
    }

    /**
     * Fetch the last wikidata update time.
     *
     * @return the date or null if there are no update times
     */
    public Date fetchLastUpdate() {
        // TODO this is very likely inefficient
        TupleQueryResult result = query("PREFIX schema: <http://schema.org/>\nSELECT (MAX(?lastUpdate) as ?maxLastUpdate)\nWHERE { ?s schema:dateModified ?lastUpdate . }");
        try {
            if (!result.hasNext()) {
                return null;
            }
            Binding maxLastUpdate = result.next().getBinding("maxLastUpdate");
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
            throw new RuntimeException("Error evaluating query", e);
        }
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

        // TODO we might want to look into Blazegraph's incremental update
        // reporting.....
        List<NameValuePair> entity = new ArrayList<>();
        entity.add(new BasicNameValuePair(type, sparql));
        post.setEntity(new UrlEncodedFormEntity(entity, Consts.UTF_8));
        try {
            try (CloseableHttpResponse response = client.execute(post)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new RuntimeException("Non-200 response from triple store:  " + response + " body=\n"
                            + responseBodyAsString(response));
                }
                return responseHandler.parse(response.getEntity());
            }
        } catch (IOException e) {
            throw new RuntimeException("Error updating triple store", e);
        }
    }

    private String responseBodyAsString(CloseableHttpResponse response) throws IOException {
        return CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
    }

    private UpdateBuilder updateBuilder() {
        UpdateBuilder b = new UpdateBuilder();
        b.prefix("entity", entityUris.namespace());
        b.prefix("schema", SchemaDotOrg.NAMESPACE);
        return b;
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
        public String acceptHeader();

        public T parse(HttpEntity entity) throws IOException;
    }

    protected static ResponseHandler<Void> IGNORE_RESPONSE = new IgnoreResponse();
    protected static ResponseHandler<TupleQueryResult> TUPLE_QUERY_RESPONSE = new TupleQueryResponse();
    protected static ResponseHandler<Boolean> ASK_QUERY_RESPONSE = new AskQueryResponse();

    protected static class IgnoreResponse implements ResponseHandler<Void> {
        @Override
        public String acceptHeader() {
            return null;
        }

        @Override
        public Void parse(HttpEntity entity) throws IOException {
            return null;
        }
    }

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