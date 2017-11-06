package org.wikidata.query.rdf.blazegraph.mwapi;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.wikidata.query.rdf.blazegraph.mwapi.ApiTemplate.OutputVariable;
import org.xml.sax.SAXException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.MockIVReturningServiceCall;
import com.google.common.collect.Iterators;

import cutthecrap.utils.striterators.ICloseableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeConstant;

/**
 * Instance of API service call.
 */
@SuppressWarnings({"rawtypes", "unchecked", "checkstyle:classfanoutcomplexity"})
@SuppressFBWarnings(value = "DMC_DUBIOUS_MAP_COLLECTION", justification = "while inputVars could be implemented as a list, the maps makes semantic sense.")
public class MWApiServiceCall implements MockIVReturningServiceCall, BigdataServiceCall {
    private static final Logger log = LoggerFactory.getLogger(MWApiServiceCall.class);

    /**
     * Query timeout, in seconds.
     * FIXME: make timeout configurable
     */
    private static final int TIMEOUT = 5;
    /**
     * Service call template.
     */
    private final ApiTemplate template;
    /**
     * List of input variable bindings.
     */
    private final Map<String, IVariableOrConstant> inputVars;
    /**
     * List of output variable bindings.
     */
    private final List<OutputVariable> outputVars;
    /**
     * HTTP connection.
     */
    private final HttpClient client;
    /**
     * The LexiconRelation for the TripleStore we're working with.
     */
    private final LexiconRelation lexiconRelation;
    /**
     * Call endpoint URL.
     */
    private final String endpoint;
    /**
     * Document builder.
     */
    private final DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    /**
     * XPath factory.
     */
    private final XPath xpath = XPathFactory.newInstance().newXPath();

    MWApiServiceCall(ApiTemplate template,
            String endpoint,
            Map<String, IVariableOrConstant> inputVars,
            List<OutputVariable> outputVars,
            HttpClient client,
            LexiconRelation lexiconRelation
    ) throws MalformedURLException, ParserConfigurationException {
        this.template = template;
        this.endpoint = new URL("https", endpoint, "/w/api.php").toExternalForm();
        this.inputVars = inputVars;
        this.outputVars = outputVars;
        this.client = client;
        this.lexiconRelation = lexiconRelation;
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return MWApiServiceFactory.SERVICE_OPTIONS;
    }

    @Override
    public ICloseableIterator<IBindingSet> call(IBindingSet[] bindingSets)
            throws Exception {
        return new MultiSearchIterator(bindingSets);
    }

    /**
     * Get parameter string from binding.
     * @param binding
     * @return
     */
    public Map<String, String> getRequestParams(IBindingSet binding) {
        final Map<String, String> params = new HashMap<>();
        // Add fixed params
        params.putAll(template.getFixedParams());
        // Resolve variable params
        for (Map.Entry<String, IVariableOrConstant> term : inputVars.entrySet()) {
            String value = null;
            IV boundValue = null;
            if (term.getValue() != null) {
                boundValue = (IV)term.getValue().get(binding);
            }
            if (boundValue == null) {
                // try default
                value = template.getInputDefault(term.getKey());
                if (value != null && value.isEmpty()) {
                    // Empty default means omit if not supplied, and it's ok
                    continue;
                }
            } else {
                value = boundValue.stringValue();
            }
            if (value == null) {
                if (template.isRequiredParameter(term.getKey())) {
                    throw new IllegalArgumentException("Could not find binding for parameter " + term.getKey());
                } else {
                    continue;
                }
            }
            params.put(term.getKey(), value);
        }

        return params;
    }

    /**
     * Get HTTP request for this particular query & binding.
     * @param binding
     * @return
     */
    private Request getHttpRequest(IBindingSet binding) {
        Request request = client.newRequest(endpoint);
        request.method(HttpMethod.GET);
        // Using XML for now to use XPath on responses
        request.param("format", "xml");
        // Add request-specific parameters
        getRequestParams(binding).forEach((key, value) -> request.param(key, value));
        return request;
    }

    @Override
    public List<IVariable<IV>> getMockVariables() {
        List<IVariable<IV>> externalVars = new LinkedList<IVariable<IV>>();
        for (OutputVariable v: outputVars) {
            externalVars.add(v.getVar());
        }
        return externalVars;
    }

    /**
     * Parse XML response from WM API.
     * @param responseStream Response body as stream
     * @param binding Current binding set.
     * @return Set of resulting bindings, or null if none found.
     * @throws SAXException on error
     * @throws IOException on error
     * @throws XPathExpressionException on error
     */
    public IBindingSet[] parseResponse(InputStream responseStream,
            IBindingSet binding) throws SAXException, IOException, XPathExpressionException  {
        if (outputVars.isEmpty()) {
            return null;
        }
        Document doc = docBuilder.parse(responseStream);
        // FIXME: we're re-compiling it each time. Should probably do it only once per template.
        // Note though that XPathExpression is not thread-safe.
        XPathExpression itemsXPath = xpath.compile(template.getItemsPath());
        NodeList nodes = (NodeList) itemsXPath.evaluate(doc, XPathConstants.NODESET);
        if (nodes.getLength() == 0) {
            return null;
        }
        IBindingSet[] results = new IBindingSet[nodes.getLength()];
        final Map<OutputVariable, XPathExpression> compiledVars = new HashMap<>();
        // TODO: would be nice to convert it to stream expression, but xpath throws,
        // and lambdas do not work with checked exceptions: xpath.compile(var.getPath()
        // Thanks, Oracle!
        for (OutputVariable var: outputVars) {
            compiledVars.put(var, xpath.compile(var.getPath()));
        }

        for (int i = 0; i < nodes.getLength(); i++) {
            final Node node = nodes.item(i);
            results[i] = binding.copy(null);
            for (Map.Entry<OutputVariable, XPathExpression> var: compiledVars.entrySet()) {
                final IConstant constant;
                if (var.getKey().isOrdinal()) {
                    constant = makeConstant(lexiconRelation.getValueFactory(), i);
                    results[i].set(var.getKey().getVar(), constant);
                    continue;
                }
                final Node value = (Node)var.getValue().evaluate(node, XPathConstants.NODE);
                if (value != null && value.getNodeValue() != null) {
                    if (var.getKey().isURI()) {
                        constant = makeConstant(lexiconRelation.getValueFactory(), var.getKey().getURI(value.getNodeValue()));
                    } else {
                        constant = makeConstant(lexiconRelation.getValueFactory(), value.getNodeValue());
                    }
                    results[i].set(var.getKey().getVar(), constant);
                }
            }
        }

        return results;
    }


    /**
     * A chunk of calls to resolve labels.
     */
    private class MultiSearchIterator implements ICloseableIterator<IBindingSet> {
        /**
         * Binding sets being resolved in this chunk.
         */
        private final IBindingSet[] bindingSets;
        /**
         * Has this chunk been closed?
         */
        private boolean closed;
        /**
         * Index of the next binding set to handle when next is next called.
         */
        private int i;
        /**
         * Current search result.
         */
        private Iterator<IBindingSet> searchResult;

        MultiSearchIterator(IBindingSet[] bindingSets) {
            this.bindingSets = bindingSets;
            searchResult = doNextSearch();
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }

            if (searchResult == null) {
                return false;
            }

            if (searchResult.hasNext()) {
                return true;
            }

            searchResult = doNextSearch();
            if (searchResult == null) {
                return false;
            } else {
                return searchResult.hasNext();
            }
        }

        /**
         * Produce next search results iterator. Skips over empty results.
         * @return Result iterator or null if no more results.
         */
        private Iterator<IBindingSet> doNextSearch() {
            // Just in case, double check
            if (closed || bindingSets == null || i >= bindingSets.length) {
                searchResult = null;
                return null;
            }
            Iterator<IBindingSet> result;
            do {
                IBindingSet binding = bindingSets[i++];
                result = doSearchFromBinding(binding);
            } while (result != null && !result.hasNext() && i < bindingSets.length);
            if (result != null && result.hasNext()) {
                return result;
            } else {
                return null;
            }
        }

        /**
         * Execute search for one specific binding set.
         * @param binding
         * @return Search results iterator.
         * @throws java.util.concurrent.TimeoutException
         * @throws InterruptedException
         */
        private Iterator<IBindingSet> doSearchFromBinding(IBindingSet binding) {
            final Request req = getHttpRequest(binding);
            log.debug("MWAPI REQUEST: {}", req.getQuery());
            final Response response;
            InputStreamResponseListener listener = new InputStreamResponseListener();
            try {
                req.send(listener);
                response = listener.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException | InterruptedException e) {
                throw new RuntimeException("MWAPI request failed", e);
            }
            if (response.getStatus() != HttpStatus.OK_200) {
                throw new RuntimeException("Bad response status: " + response.getStatus());
            }

            try {
                IBindingSet[] result = parseResponse(listener.getInputStream(), binding);
                if (result == null) {
                    return null;
                }
                return Iterators.forArray(result);
            } catch (SAXException | IOException | XPathExpressionException e) {
                throw new RuntimeException("Failed to parse response", e);
            }
        }

        @Override
        public IBindingSet next() {
            if (closed || searchResult == null) {
                return null;
            }

            if (searchResult.hasNext()) {
                return searchResult.next();
            }

            searchResult = doNextSearch();
            if (searchResult == null || !searchResult.hasNext()) {
                return null;
            } else {
                return searchResult.next();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
