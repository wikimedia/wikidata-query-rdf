package org.wikidata.query.rdf.tool.rdf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.binary.BinaryQueryResultParser;
import org.wikidata.query.rdf.common.uri.Entity;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.google.common.io.CharStreams;

public class RdfRepository {
    private final CloseableHttpClient client = HttpClientBuilder.create().build();
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
        UpdateBuilder b = new UpdateBuilder();
        b.prefix("entity", entityUris.namespace());
        b.prefix("schema", SchemaDotOrg.NAMESPACE);
        // Note that if there are any non-optional where clauses then updating
        // an empty database will never do anything
        b.whereOptional().add("?oldSiteLink", "schema:about", "entity:" + entityId)
                .add("?oldSiteLink", "?oldSiteLinkPred", "?oldSiteLinkObj");
        b.delete("?oldSiteLink", "?oldSiteLinkPred", "?oldSiteLinkObj");

        b.whereOptional().add("entity:" + entityId, "?oldSubject", "?oldObject");
        b.delete("entity:" + entityId, "?oldSubject", "?oldObject");
        for (Statement statement : statements) {
            b.insert(statement.getSubject(), statement.getPredicate(), statement.getObject());
        }
        execute("update", null, b.toString());
    }

    public TupleQueryResult query(String sparql) {
        BinaryQueryResultParser p = new BinaryQueryResultParser();
        TupleQueryResultBuilder collector = new TupleQueryResultBuilder();
        p.setQueryResultHandler(collector);
        try {
            p.parseQueryResult(execute("query", "application/x-binary-rdf-results-table", sparql).getContent());
        } catch (QueryResultParseException | QueryResultHandlerException | IllegalStateException | IOException e) {
            throw new RuntimeException("Error running query", e);
        }
        return collector.getQueryResult();
    }

    /**
     * Execute some raw SPARQL.
     * 
     * @param type name of the parameter in which to send sparql
     * @return results string from the server
     */
    protected HttpEntity execute(String type, String accept, String sparql) {
        HttpPost post = new HttpPost(uri);
        post.setHeader(new BasicHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"));
        // Note that Blazegraph totally ignores the Accept header for SPARQL
        // updates like this so the response is just html....
        if (accept != null) {
            post.setHeader(new BasicHeader("Accept", accept));
        }
        // But we set it anyway because we're ever hopeful. Also, queries.

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
                return response.getEntity();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error updating triple store", e);
        }
    }

    private String responseBodyAsString(CloseableHttpResponse response) throws IOException {
        return CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
    }
}