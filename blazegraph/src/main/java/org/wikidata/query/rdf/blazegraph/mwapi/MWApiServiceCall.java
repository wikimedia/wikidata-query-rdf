package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeConstant;
import static org.wikidata.query.rdf.common.LoggingNames.MW_API_REQUEST;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.xml.XMLConstants;
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
import org.slf4j.MDC;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
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
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import cutthecrap.utils.striterators.ICloseableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Instance of API service call.
 */
@SuppressWarnings({"rawtypes", "unchecked", "checkstyle:classfanoutcomplexity", "checkstyle:npathcomplexity"})
@SuppressFBWarnings(
        value = {"DMC_DUBIOUS_MAP_COLLECTION", "PMB_INSTANCE_BASED_THREAD_LOCAL"},
        justification = "While inputVars could be implemented as a list, the maps makes semantic sense." +
                "docBuilder as an instance ThreadLocal seems reasonable to me, especially since we expect a single long lived instance.")
public class MWApiServiceCall implements MockIVReturningServiceCall, BigdataServiceCall {
    private static final Logger LOG = LoggerFactory.getLogger(MWApiServiceCall.class);
    /**
     * Request timeout property for MWAPI requests.
     */
    private static final String TIMEOUT_PROPERTY = MWApiServiceCall.class.getName() + ".timeout";
    /**
     * Default request timeout.
     */
    private static final String TIMEOUT_MILLIS = "5000";
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
    private final Endpoint endpoint;
    /**
     * Request timeout, in ms.
     */
    private final int requestTimeout;
    /**
     * Tracker of limits on results and continuation.
     */
    private final MWApiLimits limits;
    /**
     * Thread-safe document builder.
     */
    private final ThreadLocal<DocumentBuilder> docBuilder;
    /**
     * Thread-safe xpath object.
     * We need a thread local one because XPath keeps state.
     */
    private final ThreadLocal<XPath> xpath;

    private final Timer requestTimer;

    MWApiServiceCall(ApiTemplate template, Endpoint endpoint,
                     Map<String, IVariableOrConstant> inputVars,
                     List<OutputVariable> outputVars, HttpClient client,
                     LexiconRelation lexiconRelation, Timer requestTimer,
                     MWApiLimits limits
    ) {
        this.template = template;
        this.endpoint = endpoint;
        this.inputVars = inputVars;
        this.outputVars = outputVars;
        this.client = client;
        this.lexiconRelation = lexiconRelation;
        this.requestTimer = requestTimer;
        this.requestTimeout = Integer.parseInt(System.getProperty(TIMEOUT_PROPERTY, TIMEOUT_MILLIS));
        this.limits = limits;
        this.docBuilder = ThreadLocal.withInitial(() -> {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            try {
                dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
                dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                return dbf.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                // Converting to runtime exception since anon classes + checked exceptions = :(
                LOG.error("Could not configure parser", e);
                throw new IllegalStateException(e);
            }
        });
        this.xpath = ThreadLocal.withInitial(() -> XPathFactory.newInstance().newXPath());
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return MWApiServiceFactory.SERVICE_OPTIONS;
    }

    @Override
    public ICloseableIterator<IBindingSet> call(IBindingSet[] bindingSets) {
        return new MultiSearchIterator(bindingSets);
    }

    /**
     * Get parameter string from binding.
     */
    public Map<String, String> getRequestParams(IBindingSet binding) {
        // Add fixed params
        final Map<String, String> params = new HashMap<>(template.getFixedParams());
        // Resolve variable params
        for (Map.Entry<String, IVariableOrConstant> term : inputVars.entrySet()) {
            String value;
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
                    throw new IllegalArgumentException(
                            "Could not find binding for parameter " + term.getKey());
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
     */
    private Request getHttpRequest(IBindingSet binding) {
        String endpointURL;
        try {
            endpointURL = endpoint.getEndpointURL(binding);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Bad endpoint URL", e);
        }
        if (endpointURL == null) {
            LOG.debug("MWAPI: null endpointURL");
            return null;
        }
        Request request = client.newRequest(endpointURL);
        request.timeout(requestTimeout, TimeUnit.MILLISECONDS);
        request.method(HttpMethod.GET);
        // Using XML for now to use XPath on responses
        request.param("format", "xml");
        // Add request-specific parameters
        getRequestParams(binding).forEach(request::param);
        return request;
    }

    @Override
    public List<IVariable<IV>> getMockVariables() {
        List<IVariable<IV>> externalVars = new LinkedList<>();
        for (OutputVariable v : outputVars) {
            externalVars.add(v.getVar());
        }
        return externalVars;
    }

    /**
     * Extract all continue variables from response.
     * Only api/continue is supported for now.
     *
     * @return Map with continue variables, or null if no continue.
     */
    @Nullable
    private ImmutableMap<String, String> parseContinue(Document doc, XPath xpathObject) {
        try {
            // TODO: support other options?
            XPathExpression itemsXPath = xpathObject.compile("//api/continue");
            Node continueNode = (Node) itemsXPath.evaluate(doc, XPathConstants.NODE);
            if (continueNode == null) {
                return null;
            }
            NamedNodeMap continueAttrs = continueNode.getAttributes();
            if (continueAttrs.getLength() == 0) {
                return null;
            }
            ImmutableMap.Builder<String, String> continueVars = new ImmutableMap.Builder<>();
            for (int i = 0; i < continueAttrs.getLength(); i++) {
                final Node node = continueAttrs.item(i);
                continueVars = continueVars.put(node.getNodeName(), node.getNodeValue());
            }
            return continueVars.build();
        } catch (XPathExpressionException e) {
            return null;
        }
    }

    /**
     * Parse XML response from WM API.
     *
     * @param responseStream Response body as stream
     * @param binding Current binding set.
     * @param recordsCount Count of records processed up to this batch
     * @return Set of resulting bindings, or null if none found.
     * @throws SAXException on error
     * @throws IOException on error
     * @throws XPathExpressionException on error
     */
    public ResultWithContinue parseResponse(InputStream responseStream, IBindingSet binding, int recordsCount)
            throws SAXException, IOException, XPathExpressionException {
        if (outputVars.isEmpty()) {
            LOG.debug("MWAPI: outputVars is empty");
            return null;
        }
        Document doc = docBuilder.get().parse(responseStream);
        XPath path = xpath.get();
        ImmutableMap<String, String> searchContinue = parseContinue(doc, path);
        // FIXME: we're re-compiling it each time. Should probably do it only
        // once per template.
        // Note though that XPathExpression is not thread-safe. Maybe use ThreadLocal?
        XPathExpression itemsXPath = path.compile(template.getItemsPath());
        NodeList nodes = (NodeList) itemsXPath.evaluate(doc, XPathConstants.NODESET);
        IBindingSet[] results = new IBindingSet[nodes.getLength()];
        if (results.length == 0) {
            LOG.debug("MWAPI: item xpath {} returned 0 node (empty?)", template.getItemsPath());
            return new ResultWithContinue(results, searchContinue);
        }
        final Map<OutputVariable, XPathExpression> compiledVars = new HashMap<>();
        // TODO: would be nice to convert it to stream expression, but xpath
        // throws, and lambdas do not work with checked exceptions.
        // Thanks, Oracle!
        for (OutputVariable v : outputVars) {
            compiledVars.put(v, xpath.get().compile(v.getPath()));
        }

        for (int i = 0; i < nodes.getLength(); i++) {
            final Node node = nodes.item(i);
            results[i] = binding.copy(null);
            for (Map.Entry<OutputVariable, XPathExpression> v : compiledVars.entrySet()) {
                final IConstant constant;
                if (v.getKey().isOrdinal()) {
                    constant = makeConstant(lexiconRelation.getValueFactory(), i + recordsCount);
                    results[i].set(v.getKey().getVar(), constant);
                    continue;
                }
                final Node value = (Node) v.getValue().evaluate(node, XPathConstants.NODE);
                if (value != null && value.getNodeValue() != null) {
                    if (v.getKey().isURI()) {
                        constant = makeConstant(
                                lexiconRelation.getValueFactory(),
                                v.getKey().getURI(value.getNodeValue()));
                    } else {
                        constant = makeConstant(
                                lexiconRelation.getValueFactory(),
                                value.getNodeValue());
                    }
                    results[i].set(v.getKey().getVar(), constant);
                }
            }
        }

        return new ResultWithContinue(results, searchContinue);
    }

    /**
     * A chunk of calls to resolve labels.
     * This will iterate over all results delivered for a set of bindings,
     * which will be supplied as a chunk from upstream.
     * Will use ContinueIterator for each specific IBindingSet.
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
         *
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
                result = new ContinueIterator(binding);
            } while (!result.hasNext() && i < bindingSets.length);
            if (result.hasNext()) {
                return result;
            } else {
                return null;
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

    /**
     * Iterates over the result of one query with one set of bindings.
     *
     */
    private class ContinueIterator implements ICloseableIterator<IBindingSet> {
        /**
         * Has this chunk been closed?
         */
        private boolean closed;
        /**
         * Last continuation result.
         */
        private ResultWithContinue lastResult;
        /**
         * Current bindings.
         */
        private IBindingSet bindings;
        /**
         * Count of records sent out.
         */
        private int recordsCount;

        ContinueIterator(IBindingSet binding) {
            this.bindings = binding;
            lastResult = doSearchRequest(0);
        }

        /**
         * This performs actual HTTP request to Mediawiki API.
         * This can be called several times if continue is present.
         * @param currentRecordsCount Count of records processed up to this batch.
         * @return Query results with continue structure.
         */
        private ResultWithContinue doSearchRequest(int currentRecordsCount) {
            final Request req = getHttpRequest(bindings);
            if (req == null) {
                return null;
            }
            if (lastResult != null && lastResult.getContinue() != null) {
                lastResult.getContinue().forEach(req::param);
            }
            try (Closeable mdc = MDC.putCloseable(MW_API_REQUEST, req.getQuery())) {
                LOG.debug("MWAPI REQUEST: {}", req.getQuery());
                final Response response;
                InputStreamResponseListener listener = new InputStreamResponseListener();

                try (Timer.Context context = requestTimer.time()) {
                    req.send(listener);
                    response = listener.get(requestTimeout, TimeUnit.MILLISECONDS);
                    LOG.debug("MWAPI RESPONSE: HTTP {}, HEADERS: {}", response.getStatus(), response.getHeaders());
                }

                if (response.getStatus() != HttpStatus.OK_200) {
                    RuntimeException r = new RuntimeException("Bad response status: " + response.getStatus());
                    response.abort(r);
                    throw r;
                }
                try (InputStream inputStream = listener.getInputStream()) {
                    return parseResponse(inputStream, bindings, currentRecordsCount);
                }
            } catch (InterruptedException e) {
                LOG.debug("MWAPI REQUEST ERR: {}", req.getQuery(), e);
                Thread.currentThread().interrupt();
                throw new RuntimeException("MWAPI request failed", e);
            } catch (ExecutionException | TimeoutException  e) {
                LOG.debug("MWAPI REQUEST ERR: {}", req.getQuery(), e);
                throw new RuntimeException("MWAPI request failed", e);
            } catch (SAXException | IOException | XPathExpressionException e) {
                LOG.debug("MWAPI REQUEST ERR: {}", req.getQuery(), e);
                throw new RuntimeException("Failed to parse response", e);
            } finally {
                LOG.debug("MWAPI REQUEST END: {}", req.getQuery());
            }
        }

        @Override
        public boolean hasNext() {
            if (closed || lastResult == null) {
                return false;
            }
            if (!limits.allowResult()) {
                return false;
            }
            return lastResult.getResultIterator().hasNext() ||
                (limits.allowContinuation() && lastResult.searchContinue != null);
        }

        @Override
        public IBindingSet next() {
            if (!limits.allowResult()) {
                close();
            }
            if (closed || lastResult == null) {
                return null;
            }
            if (lastResult.getResultIterator().hasNext()) {
                limits.haveResult();
                recordsCount++;
                return lastResult.getResultIterator().next();
            }
            // If we can continue, do the continue
            if (lastResult.getContinue() != null && limits.allowContinuation()) {
                lastResult = doSearchRequest(recordsCount);
                limits.haveContinuation();
            }
            if (closed || lastResult == null) {
                return null;
            }
            if (lastResult.getResultIterator().hasNext()) {
                limits.haveResult();
                recordsCount++;
                return lastResult.getResultIterator().next();
            }
            return null;
        }

        @Override
        public void close() {
            closed = true;
        }

    }

    public static class ResultWithContinue {
        /**
         * Search continuation.
         */
        private final ImmutableMap<String, String> searchContinue;
        /**
         * Iterator over search result.
         */
        private final UnmodifiableIterator<IBindingSet> resultIterator;

        ResultWithContinue(IBindingSet[] searchResult, ImmutableMap<String, String> searchContinue) {
            this.searchContinue = searchContinue;
            this.resultIterator = Iterators.forArray(searchResult);
        }

        public Iterator<IBindingSet> getResultIterator() {
            return resultIterator;
        }

        public Map<String, String> getContinue() {
            return searchContinue;
        }
    }
}
