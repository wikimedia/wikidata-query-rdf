package org.wikidata.query.rdf.blazegraph.ldf;

import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Value;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForTriplePatterns;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * A Blazegraph-based data source.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class BlazegraphBasedTPFRequestProcessor extends
            AbstractRequestProcessorForTriplePatterns<BigdataValue, String, String> {
    /**
     * Blazegraph data store.
     */
    private final AbstractTripleStore store;

    public BlazegraphBasedTPFRequestProcessor(final AbstractTripleStore store) {
        this.store = store;
    }

    @Override
    protected Worker getTPFSpecificWorker(
            final ITriplePatternFragmentRequest<BigdataValue, String, String> req)
                    throws IllegalArgumentException {
        return new Worker(req);
    }

    /**
     * Blazegraph worker class.
     */
    protected class Worker extends
                AbstractRequestProcessorForTriplePatterns.Worker<BigdataValue, String, String> {
        public Worker(
                final ITriplePatternFragmentRequest<BigdataValue, String, String> req) {
            super(req);
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        @Override
        protected ILinkedDataFragment createFragment(
                final ITriplePatternElement<BigdataValue, String, String> s,
                final ITriplePatternElement<BigdataValue, String, String> p,
                final ITriplePatternElement<BigdataValue, String, String> o,
                final long offset, final long limit) {
            int numOfValues = 0;
            if (!s.isVariable())
                numOfValues++;
            if (!p.isVariable())
                numOfValues++;
            if (!o.isVariable())
                numOfValues++;

            if (numOfValues > 0) {
                final BigdataValue[] values = new BigdataValue[numOfValues];
                int i = 0;
                if (!s.isVariable())
                    values[i++] = s.asConstantTerm();
                if (!p.isVariable())
                    values[i++] = p.asConstantTerm();
                if (!o.isVariable())
                    values[i++] = o.asConstantTerm();

                // Initialize the IVs of the values.
                store.getLexiconRelation().addTerms(values, numOfValues, true); // readOnly=true

                // Check if some IVs have not been initialized. In this case
                // the corresponding values are not used in any triple in the
                // store. Hence, there cannot be any matching triple for the
                // requested triple pattern and, thus, we return an empty TPF.
                for (int j = 0; j < numOfValues; j++) {
                    @SuppressWarnings("rawtypes")
                    final IV iv = values[j].getIV();
                    if (iv == null || (iv instanceof TermId)
                            && ((TermId<?>) iv).getTermId() == 0L)
                        return createEmptyTriplePatternFragment();
                }
            }

            final VariablesBasedFilter filter = createFilterIfNeeded(s, p, o);
            final IAccessPath<ISPO> ap = store.getSPORelation().getAccessPath(
                    asIVorNull(s), asIVorNull(p), asIVorNull(o), null, // c=null
                    filter);

            return createTriplePatternFragment(ap, offset, limit);
        }

        /**
         * Create filter based on triple request.
         * @param s
         * @param p
         * @param o
         * @return
         */
        @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
        public VariablesBasedFilter createFilterIfNeeded(
                final ITriplePatternElement<BigdataValue, String, String> s,
                final ITriplePatternElement<BigdataValue, String, String> p,
                final ITriplePatternElement<BigdataValue, String, String> o) {
            if (!s.isSpecificVariable() && !p.isSpecificVariable()
                    && !o.isSpecificVariable())
                return null;

            final Set<String> varNames = new HashSet<String>();

            if (s.isNamedVariable()) {
                varNames.add(s.asNamedVariable());
            }

            if (p.isNamedVariable()) {
                if (varNames.contains(p.asNamedVariable()))
                    return new VariablesBasedFilter(s, p, o);

                varNames.add(p.asNamedVariable());
            }

            if (o.isNamedVariable() && varNames.contains(o.asNamedVariable())) {
                return new VariablesBasedFilter(s, p, o);
            }

            varNames.clear();

            if (s.isAnonymousVariable()) {
                varNames.add(s.asAnonymousVariable());
            }

            if (p.isAnonymousVariable()) {
                if (varNames.contains(p.asAnonymousVariable()))
                    return new VariablesBasedFilter(s, p, o);

                varNames.add(p.asAnonymousVariable());
            }

            if (o.isAnonymousVariable()
                    && varNames.contains(o.asAnonymousVariable())) {
                return new VariablesBasedFilter(s, p, o);
            }

            return null;
        }

        /**
         * Create the fragment.
         * @param ap Access path
         * @param offset Offset
         * @param limit Limit
         * @return
         */
        protected ITriplePatternFragment createTriplePatternFragment(
                final IAccessPath<ISPO> ap, final long offset,
                final long limit) {
            final long count = ap.rangeCount(false); // exact=false, i.e., fast
                                                     // range count

            if (count == 0L) {
                return createEmptyTriplePatternFragment();
            } else {
                return new BlazegraphBasedTPF(ap, store, count, offset, limit,
                        request.getFragmentURL(), request.getDatasetURL(),
                        request.getPageNumber());
            }
        }

    } // end of class Worker

    /**
     * Convert element to IV if it's constant.
     * @param tpe
     * @return IV or null if not constant.
     */
    @SuppressWarnings("rawtypes")
    public static IV asIVorNull(
            final ITriplePatternElement<BigdataValue, String, String> tpe) {
        return (tpe.isVariable()) ? null : tpe.asConstantTerm().getIV();
    }

    /**
     * Filter based on variables.
     */
    public static class VariablesBasedFilter extends SPOFilter<ISPO> {
        private static final long serialVersionUID = 6979067019748992496L;

        /**
         * Check subject.
         */
        private final boolean checkS;
        /**
         * Check predicate.
         */
        private final boolean checkP;
        /**
         * Check object.
         */
        private final boolean checkO;

        @SuppressWarnings({"checkstyle:localvariablename", "checkstyle:cyclomaticcomplexity"})
        public VariablesBasedFilter(
                final ITriplePatternElement<BigdataValue, String, String> s,
                final ITriplePatternElement<BigdataValue, String, String> p,
                final ITriplePatternElement<BigdataValue, String, String> o) {
            boolean _checkS = false;
            boolean _checkP = false;
            boolean _checkO = false;

            if (s.isNamedVariable()) {
                final String sVarName = s.asNamedVariable();
                if (p.isNamedVariable()
                        && p.asNamedVariable().equals(sVarName)) {
                    _checkS = true;
                    _checkP = true;
                }

                if (o.isNamedVariable()
                        && o.asNamedVariable().equals(sVarName)) {
                    _checkS = true;
                    _checkO = true;
                }
            }

            if (s.isAnonymousVariable()) {
                final String sVarName = s.asAnonymousVariable();
                if (p.isAnonymousVariable()
                        && p.asAnonymousVariable().equals(sVarName)) {
                    _checkS = true;
                    _checkP = true;
                }

                if (o.isAnonymousVariable()
                        && o.asAnonymousVariable().equals(sVarName)) {
                    _checkS = true;
                    _checkO = true;
                }
            }

            if (p.isNamedVariable() && o.isNamedVariable()
                    && p.asNamedVariable().equals(o.asNamedVariable())) {
                _checkP = true;
                _checkO = true;
            }

            if (p.isAnonymousVariable() && o.isAnonymousVariable() && p
                    .asAnonymousVariable().equals(o.asAnonymousVariable())) {
                _checkP = true;
                _checkO = true;
            }

            checkS = _checkS;
            checkP = _checkP;
            checkO = _checkO;
        }

        @Override
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public boolean isValid(Object obj) {
            if (!canAccept(obj))
                return true;

            final ISPO spo = (ISPO) obj;
            final Value s = spo.getSubject();
            final Value p = spo.getPredicate();
            final Value o = spo.getObject();

            if (checkS && checkP && !s.equals(p))
                return false;

            if (checkS && checkO && !s.equals(o))
                return false;

            if (checkP && checkO && !p.equals(o))
                return false;

            return true;
        }

    } // end of class VariablesBasedFilter

}
