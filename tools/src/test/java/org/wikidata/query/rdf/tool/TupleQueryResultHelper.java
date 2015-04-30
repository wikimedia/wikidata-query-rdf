package org.wikidata.query.rdf.tool;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

/**
 * Utility functions for {@link TupleQueryResult}s.
 */
public final class TupleQueryResultHelper {

    /**
     * Convert a {@link TupleQueryResult} into an iterable of BindingSets.
     *
     * @param results
     * @return
     * @throws QueryEvaluationException
     *             if something goes really wrong
     */
    public static Iterable<BindingSet> toIterable(TupleQueryResult results) throws QueryEvaluationException {
        List<BindingSet> buffer = new ArrayList<>();
        while (results.hasNext()) {
            buffer.add(results.next());
        }
        return buffer;
    }

    /**
     * Utility class.
     */
    private TupleQueryResultHelper() {
    }
}
