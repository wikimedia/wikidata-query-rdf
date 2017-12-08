package org.wikidata.query.rdf.tool;

import java.net.URI;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;

/**
 * RdfRepository extension used for testing. We don't want to anyone to
 * accidentally use clear() so we don't put it in the repository.
 */
public class RdfRepositoryForTesting extends RdfRepository implements TestRule {

    /**
     * The namespace of the local RDF repository, e.g. "kb" or "wdq".
     */
    private final String namespace;

    public RdfRepositoryForTesting(String namespace) {
        super(url("/namespace/" + namespace + "/sparql"), WikibaseUris.WIKIDATA);
        this.namespace = namespace;
    }

    /**
     * Take a relative path and create a URL with the full path to Blazegraph on
     * localhost.
     */
    private static URI url(String path) {
        return URI.create("http://localhost:9999/bigdata" + path);
    }

    /**
     * Clear's the whole repository.
     */
    public void clear() {
        execute("update", RdfRepository.UPDATE_COUNT_RESPONSE, "CLEAR ALL");
    }

    /**
     * Loads a uri into this rdf repository. Uses Blazegraph's update with
     * uri's feature.
     */
    public int loadUrl(String uri) {
        return execute("uri", RdfRepository.UPDATE_COUNT_RESPONSE, uri);
    }

    /**
     * Updates the repository.
     */
    public int update(String query) {
        return execute("update", RdfRepository.UPDATE_COUNT_RESPONSE, query);
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
    private void before() {
        clear();
    }

    /**
     * Clear and close repository after test.
     */
    private void after() throws Exception {
        clear();
        close();
    }
}
