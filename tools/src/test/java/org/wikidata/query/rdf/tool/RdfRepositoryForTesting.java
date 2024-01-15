package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClient;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClientRetryer;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyHost;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyPort;
import static org.wikidata.query.rdf.tool.Update.getRdfClientTimeout;

import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import com.google.common.annotations.VisibleForTesting;

/**
 * RdfRepository extension used for testing. We don't want to anyone to
 * accidentally use clear() so we don't put it in the repository.
 */
public class RdfRepositoryForTesting extends RdfRepository implements TestRule {

    /**
     * @param namespace The namespace of the local RDF repository, e.g. "kb" or "wdq".
     */
    public RdfRepositoryForTesting(String namespace) {
        super(
                UrisSchemeFactory.WIKIDATA,
                new RdfClient(
                        buildHttpClient(getHttpProxyHost(), getHttpProxyPort()), url("/namespace/" + namespace + "/sparql"),
                        buildHttpClientRetryer(),
                        getRdfClientTimeout(),
                        RdfClient.DEFAULT_MAX_RESPONSE_SIZE
                ),
                20_000_000
        );
    }

    /**
     * Take a relative path and create a URL with the full path to Blazegraph on
     * localhost.
     */
    public static URI url(String path) {
        return URI.create("http://localhost:9999/bigdata" + path);
    }

    /**
     * Clear's the whole repository.
     */
    public void clear() {
        rdfClient.update("CLEAR ALL");
    }

    /**
     * Updates the repository.
     */
    public int update(String query) {
        return rdfClient.update(query);
    }

    /**
     * Delete the given namespace from the test Blazegraph server.
     */
    protected void deleteNamespace() {
//            HttpDelete delete = new HttpDelete(url("/namespace/" + namespace));
//            submit(delete, 200);
    }

    /**
     * Create the given namespace in the test Blazegraph server.
     */
    protected void createNamespace() {
//            HttpPost post = new HttpPost(url("/namespace"));
//            post.setHeader(new BasicHeader("Content-Type", "application/xml; charset=UTF-8"));
//            StringBuilder body = new StringBuilder();
//            body.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>");
//            body.append("<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">");
//            body.append("<properties>");
//            body.append("<entry key=\"com.bigdata.rdf.sail.namespace\">").append(namespace).append("</entry>");
//            body.append("<entry key=\"com.bigdata.rdf.store.AbstractTripleStore.textIndex\">false</entry>");
//            body.append("<entry key=\"com.bigdata.rdf.sail.truthMaintenance\">true</entry>");
//            body.append("<entry key=\"com.bigdata.rdf.store.AbstractTripleStore.quads\">false</entry>");
//            body.append("<entry key=\"com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers\">false</entry>");
//            body.append("<entry key=\"com.bigdata.rdf.store.AbstractTripleStore.axiomsClass\">com.bigdata.rdf.axioms.NoAxioms</entry>");
//            body.append("</properties>");
//            post.setEntity(new StringEntity(body.toString(), "UTF-8"));
//            submit(post, 201);
    }

    /** {@inheritDoc} */
    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                try {
                    base.evaluate();
                } finally {
                    after();
                }
            }
        };
    }

    /**
     * Clear repository before tests.
     */
    public void before() {
        clear();
    }

    /**
     * Clear and close repository after test.
     */
    public void after() throws Exception {
        clear();
        rdfClient.httpClient.stop();
    }

    /**
     * @deprecated tests should be refactored to test RdfClient directly
     */
    @Deprecated
    @VisibleForTesting
    public TupleQueryResult query(String sparql) {
        return rdfClient.query(sparql);
    }

    /**
     * @deprecated tests should be refactored to test RdfClient directly
     */
    @Deprecated
    @VisibleForTesting
    public boolean ask(String sparql) {
        return rdfClient.ask(sparql);
    }

    public RdfClient getClient() {
        return rdfClient;
    }

    /**
     * Overridden sync method.
     * Selects which sync to use - one-value or multi-value, to ensure both work the same.
     * FIXME: there should be two cleanup list params here, values & refs
     */
    public int sync(String entityId, Collection<org.openrdf.model.Statement> statements, Collection<String> valueList) {
        return multiSync(entityId, statements, valueList);
    }

    /**
     * Overridden sync method.
     * Selects which sync to use - one-value or multi-value, to ensure both work the same.
     */
    public int sync(String entityId, Collection<org.openrdf.model.Statement> statements) {
        return multiSync(entityId, statements, Collections.emptyList());
    }

    /**
     * Run sync for single ID via multi-change API.
     */
    private int multiSync(String entityId, Collection<org.openrdf.model.Statement> statements, Collection<String> valueList) {
        Change change = new Change(entityId, -1, Instant.now(), -1);
        change.setStatements(statements);
        // FIXME: we should not conflate refs&values cleanups like that in test
        change.setValueCleanupList(valueList);
        change.setRefCleanupList(valueList);
        int res = syncFromChanges(Collections.singleton(change), false).getMutationCount();
        // This is because many tests do not know about timestamps which are later addition.
        // This is the easiest way to make them ignore timestamps without complicating syncFromChanges too much.
        int ts = rdfClient.update("DELETE { ?x wikibase:timestamp ?y } WHERE { ?x wikibase:timestamp ?y }");
        return res - ts;
    }
}
