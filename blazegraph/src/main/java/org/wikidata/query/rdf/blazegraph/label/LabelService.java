package org.wikidata.query.rdf.blazegraph.label;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AbstractServiceFactory;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.striterator.IChunkedOrderedIterator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Implements a "service" that resolves label like things in a way that doesn't
 * change the cardinality of the result set. You can call it like this: <code>
 *  SELECT *
 *  WHERE {
 *    SERVICE wikibase:label {
 *      bd:serviceParam wikibase:language "en,de,fr" .
 *      wd:Q123 rdfs:label ?q123Label .
 *      wd:Q123 rdfs:altLabel ?q123Alt .
 *      wd:Q123 schema:description ?q123Desc .
 *      wd:Q321 rdf:label ?q321Label .
 *    }
 *  }
 * </code> or like this:<code>
 *  SELECT ?sLabel ?sAltLabel ?sDescription ?oLabel
 *  WHERE {
 *    ?s wdt:P22 ?o .
 *    SERVICE wikibase:label {
 *      bd:serviceParam wikibase:language "en,de" .
 *      bd:serviceParam wikibase:language "fr" .
 *    }
 *  }
 * </code>
 * <p>
 * If the label isn't available in any of the fallback languages it'll come back
 * as the entityId. Alt labels and descriptions just come back unbound if they
 * don't exist. If multiple values are defined they come back as a comma
 * separated list.
 *
 * <p>
 * This works by resolving the label-like thing per incoming binding, one at a
 * time. It would probably be faster to do something bulkish but we don't do
 * that yet. The code to do the comma separated lists and entityIds is pretty
 * simple once you've resolve the data.
 * <p>
 * The second invocation pattern works using {@code EmptyLabelServiceOptimizer}
 * to inspect the query and automatically build the first form out of the second
 * form by inspecting the query's projection.
 */
public class LabelService extends AbstractServiceFactory {
    private static final Logger log = Logger
            .getLogger(LabelService.class);

    /**
     * Options configuring this service as a native Blazegraph service.
     */
    private static final BigdataNativeServiceOptions SERVICE_OPTIONS = new BigdataNativeServiceOptions();

    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(Ontology.LABEL);

    /**
     * URI for service language parameter.
     */
    private static final URIImpl LANGUAGE_PARAM = new URIImpl(Ontology.NAMESPACE + "language");

    /**
     * Register the service so it is recognized by Blazegraph.
     */
    public static void register() {
        final ServiceRegistry reg = ServiceRegistry.getInstance();
        reg.add(SERVICE_KEY, new LabelService());
        reg.addWhitelistURL(SERVICE_KEY.toString());
    }

    @Override
    public IServiceOptions getServiceOptions() {
        return SERVICE_OPTIONS;
    }

    @Override
    public BigdataServiceCall create(ServiceCallCreateParams params, final ServiceParams serviceParams) {
        /*
         * Luckily service calls are always pushed to the last operation in a
         * query. We still check it and tell users we won't resolve labels for
         * unbound subjects.
         */
        // TODO this whole class just throws RuntimeException instead of ??
        return new LabelServiceCall(new ResolutionContext(params.getTripleStore(), findLanguageFallbacks(serviceParams)),
                findResolutions(params));
    }

    /**
     * Resolve the language fallbacks from the statement pattern node in the
     * query.
     */
    private Map<String, Integer> findLanguageFallbacks(final ServiceParams params) {
        List<TermNode> paramNodes = params.get(LANGUAGE_PARAM);

        if (paramNodes.size() < 1) {
            throw new IllegalArgumentException("You must provide the label service a list of languages.");
        }

        // TODO there has to be a better data structure for this.
        /*
         * Lucene has tons of things for this, but yeah. Maybe it doesn't
         * matter.
         */
        Map<String, Integer> fallbacksMap = new HashMap<>();
        int cnt = 0;
        for (TermNode term: paramNodes) {
            if (term.isVariable()) {
                throw new IllegalArgumentException("not a constant");
            }

            final Value v = term.getValue();

            if (!(v instanceof Literal)) {
                throw new IllegalArgumentException("not a literal");
            }

            final String s = v.stringValue();
            if (s.contains(",")) {
                // we also allow comma lists for convenience
                for (String ls: s.split(",")) {
                    fallbacksMap.put(ls.trim(), cnt);
                    ++cnt;
                }
            } else {
                fallbacksMap.put(s.trim(), cnt);
            }
            ++cnt;
        }

        return fallbacksMap;
    }

    /**
     * Create the resolutions list from the service call parameters.
     */
    private List<Resolution> findResolutions(ServiceCallCreateParams params) {
        JoinGroupNode g = (JoinGroupNode) params.getServiceNode().getGraphPattern();
        List<Resolution> resolutions = new ArrayList<>(g.args().size());
        for (BOp st : g.args()) {
            StatementPatternNode sn = (StatementPatternNode) st;
            if (sn.s().isConstant() && BD.SERVICE_PARAM.equals(sn.s().getValue())) {
                // skip service params
                continue;
            }
            resolutions.add(new Resolution(sn));
        }
        return resolutions;
    }

    /**
     * Represents the call site in a particular SPARQL query.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    private static class LabelServiceCall implements BigdataServiceCall {
        /*
         * Suppress VisibilityModifier check because members are package private
         * so non-static inner classes can access them without the messy
         * accessor methods. This isn't an information leak because this class
         * is already a private inner class.
         */
        /**
         * The context in which the resolutions will be done.
         */
        final ResolutionContext context;
        /**
         * Things to resolve.
         */
        final List<Resolution> resolutions;

        /**
         * Build with all the right stuff resolved.
         */
        public LabelServiceCall(ResolutionContext context, List<Resolution> resolutions) {
            this.context = context;
            this.resolutions = resolutions;
        }

        @Override
        public IServiceOptions getServiceOptions() {
            return SERVICE_OPTIONS;
        }

        @Override
        public ICloseableIterator<IBindingSet> call(final IBindingSet[] bindingSets) throws Exception {
            return new Chunk(bindingSets);
        }

        /**
         * A chunk of calls to resolve labels.
         */
        private class Chunk implements ICloseableIterator<IBindingSet> {
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

            public Chunk(IBindingSet[] bindingSets) {
                this.bindingSets = bindingSets;
            }

            @Override
            public boolean hasNext() {
                return !closed && i < bindingSets.length;
            }

            @Override
            public IBindingSet next() {
                IBindingSet binding = bindingSets[i++];
                context.binding(binding);
                for (Resolution resolution : resolutions) {
                    context.resolve(resolution);
                }
                return binding;
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

    /**
     * Description of a specific resolution request for the service. The service
     * can resolve many such requests at once.
     */
    @SuppressWarnings("rawtypes")
    private static final class Resolution {
        /**
         * Subject of the service call.
         */
        private final IValueExpression subject;
        /**
         * URI for the label to resolve.
         */
        private final IValueExpression label;
        /**
         * The target variable to which to bind the label.
         */
        private final IVariable target;

        private Resolution(StatementPatternNode st) {
            subject = st.s().getValueExpression();
            label = st.p().getValueExpression();
            target = getVariableToBind(st);
        }

        /**
         * Subject of the service call.
         */
        public IValueExpression subject() {
            return subject;
        }

        /**
         * URI for the label to resolve.
         */
        public IValueExpression labelType() {
            return label;
        }

        /**
         * The target variable to which to bind the label.
         */
        public IVariable target() {
            return target;
        }

        /**
         * Resolve the variable that needs to be bound from the statement
         * pattern node in the query.
         */
        private IVariable<IV> getVariableToBind(StatementPatternNode st) {
            try {
                return ((VarNode) st.o()).getValueExpression();
            } catch (ClassCastException e) {
                throw new RuntimeException("Expected a variable in the object position to which to bind the language.");
            }
        }
    }

    /**
     * Context in which Resolutions are resolved. This only goes from subjects
     * and label types to labels. It doesn't go from label types and label
     * values to subjects. That wouldn't be an efficient process anyway even
     * though it is technically possible.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static class ResolutionContext {
        /**
         * The TripleStore to resolve the BindingSets against.
         */
        private final AbstractTripleStore tripleStore;
        /**
         * The LexiconRelation for the TripleStore we're working with.
         */
        private final LexiconRelation lexiconRelation;
        /**
         * The language fallbacks as a map from language code to order of
         * precidence.
         */
        private final Map<String, Integer> languageFallbacks;
        /**
         * List of labels with the best language. Cleared and rebuilt as on
         * every new call to resolve.
         */
        private final List<IV> bestLabels = new ArrayList<>();
        /**
         * The binding currently being resolved.
         */
        private IBindingSet binding;
        /**
         * The subject for this resolution as resolved in this BindingSet.
         */
        private IV resolvedSubject;
        /**
         * The label type for the current resolution as resolved in this
         * BindingSet.
         */
        private IV resolvedLabelType;
        /**
         * The IV the represents rdfs:label. Its built lazily when needed and
         * cached.
         */
        private IV rdfsLabelIv;

        public ResolutionContext(AbstractTripleStore tripleStore, Map<String, Integer> languageFallbacks) {
            this.tripleStore = tripleStore;
            this.languageFallbacks = languageFallbacks;
            lexiconRelation = tripleStore.getLexiconRelation();
        }

        /**
         * Set the current BindingSet to be worked on.
         */
        public void binding(IBindingSet binding) {
            this.binding = binding;
        }

        /**
         * Resolve the target of the resolution in the current BindingSet.
         */
        public void resolve(Resolution resolution) {
            resolvedSubject = resolveToIvOrError(resolution.subject(), "subject");
            resolvedLabelType = resolveToIvOrError(resolution.labelType(), "label type");
            if (resolvedSubject == null || resolvedLabelType == null) {
                return;
            }
            // TODO this is one at a time - maybe a batch things?
            fillBestLabels();
            IV label = pickOrBuildBestLabel();
            if (label != null) {
                binding.set(resolution.target(), new Constant(label));
            }
        }

        /**
         * Gets the best label from a lookup. The best labels are put into the
         * bestLabels list parameter. That parameter is cleared before the
         * method starts and returning an empty list means there are no good
         * labels.
         */
        private void fillBestLabels() {
            IChunkedOrderedIterator<ISPO> lookup = tripleStore.getAccessPath(resolvedSubject, resolvedLabelType, null)
                    .iterator();
            try {
                bestLabels.clear();
                int bestLabelRank = Integer.MAX_VALUE;
                while (lookup.hasNext()) {
                    ISPO spo = lookup.next();
                    IV o = spo.o();
                    if (!o.isLiteral()) {
                        // Not a literal, no chance its a label then
                        continue;
                    }
                    /*
                     * Hydrate all of the objects into language literals so we
                     * can check the language. This is slow because it has to go
                     * to the term dictionary but there isn't anything we can do
                     * about it for now.
                     */
                    Literal literal = (Literal) lexiconRelation.getTerm(o);
                    String language = literal.getLanguage();
                    if (language == null) {
                        // Not a language label, skip.
                        continue;
                    }
                    Integer languageOrdinal = languageFallbacks.get(language);
                    if (languageOrdinal == null) {
                        // Not a language the user wants
                        continue;
                    }
                    if (languageOrdinal == bestLabelRank) {
                        bestLabels.add(o);
                    }
                    if (languageOrdinal < bestLabelRank) {
                        bestLabelRank = languageOrdinal;
                        bestLabels.clear();
                        bestLabels.add(o);
                    }
                }
            } finally {
                lookup.close();
            }
        }

        /**
         * By hook or by crook return a single IV for this resolution. Processes
         * bestLabels, so you'll have to call fillBestLabels before calling
         * this. Options:
         * <ul>
         * <li>If there is a single label it returns it.
         * <li>If there isn't a label it uses bestEffortLabel to mock up a label
         * <li>If there are multiple labels it uses joinLabels to smoosh them
         * into a comma separated list.
         * </ul>
         */
        private IV pickOrBuildBestLabel() {
            switch (bestLabels.size()) {
            case 1:
                // Found a single label so we can just return it.
                // This is probably the most common case.
                return bestLabels.get(0);
            case 0:
                // Didn't find a real label so lets fake one up
                return bestEffortLabel();
            default:
                return joinLabels();
            }
        }

        /**
         * Build a mock IV from a literal.
         */
        private IV mock(Literal literal) {
            TermId mock = TermId.mockIV(VTE.LITERAL);
            mock.setValue(lexiconRelation.getValueFactory().asValue(literal));
            return mock;
        }

        /**
         * Returns the IV to which expression is bound in the current context or
         * throws an error if it isn't bound.
         */
        private IV resolveToIvOrError(IValueExpression expression, String nameOfExpression) {
            Object resolved = expression.get(binding);
            if (resolved == null) {
                return null;
//                throw new RuntimeException(String.format(Locale.ROOT,
//                        "Refusing to lookup labels for unknown %s (%s). That'd be way way way inefficient.",
//                        nameOfExpression, expression));
            }
            try {
                return (IV) resolved;
            } catch (ClassCastException e) {
                throw new RuntimeException(String.format(Locale.ROOT,
                        "Expected %s (%s) to be bound to an IV but it wasn't.", nameOfExpression, expression));
            }
        }

        /**
         * The IV the represents rdfs:label. Its built lazily when needed and
         * cached.
         */
        public IV rdfsLabelIv() {
            if (rdfsLabelIv == null) {
                rdfsLabelIv = tripleStore.getVocabulary().get(RDFS.LABEL);
            }
            return rdfsLabelIv;
        }

        /**
         * The WikibaseUris to use in this context.
         */
        private WikibaseUris uris() {
            // TODO lookup wikibase host and default to wikidata
            return WikibaseUris.getURISystem();
        }

        /**
         * Build a label for something without a label. If the resolvedLabelType
         * is actually rdfs:label you'll get a nice Q1324 style label but if it
         * isn't you'll get an empty string.
         */
        private IV bestEffortLabel() {
            // Only rdfs:label gets the entity ID as the label
            if (!rdfsLabelIv().equals(resolvedLabelType)) {
                // Everything else gets the empty string
                return null;
            }
            BigdataValue value = lexiconRelation.getTerm(resolvedSubject);
            String bestEffortLabel = value.stringValue();
            if (bestEffortLabel.startsWith(uris().entity())) {
                bestEffortLabel = bestEffortLabel.substring(uris().entity().length());
            }
            return mock(new LiteralImpl(bestEffortLabel));
        }

        /**
         * Smoosh bestLabels into a comma separated list.
         */
        private IV joinLabels() {
            // Found lots of labels so we should merge them into one.
            // This is going to be common for alt labels
            StringBuilder b = new StringBuilder();
            String language = null;
            boolean first = true;
            for (IV label : bestLabels) {
                Literal literal = (Literal) lexiconRelation.getTerm(label);
                if (!first) {
                    b.append(", ");
                } else {
                    first = false;
                }
                b.append(literal.stringValue());
                if (language == null) {
                    language = literal.getLanguage();
                }
            }
            return mock(new LiteralImpl(b.toString(), language));
        }
    }
}
