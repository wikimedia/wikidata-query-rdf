package org.wikidata.query.rdf.blazegraph.mwapi;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

/**
 * Tracker for limits to MediaWiki API calls.
 * Implements a limit on the total number of results,
 * one on the total number of continuations,
 * and one on the number of consecutive continuations that returned no result
 * (“empty” continuations; only possible if
 * <a href="https://www.mediawiki.org/wiki/Manual:$wgMiserMode">miser mode</a>
 * is enabled on the target wiki).
 */
@NotThreadSafe
public class MWApiLimits {

    @VisibleForTesting
    final int limitResults;

    @VisibleForTesting
    final int limitContinuations;

    @VisibleForTesting
    final int limitEmptyContinuations;

    private int results;

    private int continuations;

    private int emptyContinuations;

    /**
     * Create a new tracker instance.
     *
     * @param limitResults maximum number of results to fetch
     * @param limitContinuations maximum number of continuations to follow
     * @param limitEmptyContinuations maximum number of consecutive empty continuations to follow
     */
    public MWApiLimits(int limitResults, int limitContinuations, int limitEmptyContinuations) {
        this.limitResults = limitResults;
        this.limitContinuations = limitContinuations;
        this.limitEmptyContinuations = limitEmptyContinuations;
    }

    /**
     * Check whether one more API result should be allowed,
     * without any change to the state of the tracker.
     */
    public boolean allowResult() {
        return results < limitResults;
    }

    /**
     * Inform the tracker that one more API result has been retrieved,
     * and adjust the internal counters accordingly.
     */
    public void haveResult() {
        emptyContinuations = 0;
        results++;
    }

    /**
     * Check whether one more continuation should be allowed,
     * without any change to the state of the tracker.
     */
    public boolean allowContinuation() {
        return continuations < limitContinuations &&
            emptyContinuations < limitEmptyContinuations;
    }

    /**
     * Inform the tracker that one more continuation has been performed,
     * and adjust the internal counters accordingly.
     */
    public void haveContinuation() {
        continuations++;
        emptyContinuations++;
    }

}
