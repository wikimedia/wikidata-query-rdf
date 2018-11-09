package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MWApiLimitsUnitTest {

    @Test
    public void testLimitResults() {
        MWApiLimits limits = new MWApiLimits(3, 1000, 1000);

        assertTrue(limits.allowResult());
        limits.haveResult();

        assertTrue(limits.allowResult());
        limits.haveResult();

        limits.haveContinuation();

        assertTrue(limits.allowResult());
        limits.haveResult();

        assertFalse(limits.allowResult());
    }

    @Test
    public void testLimitContinuations() {
        MWApiLimits limits = new MWApiLimits(10000, 3, 1000);

        limits.haveResult();
        assertTrue(limits.allowContinuation());
        limits.haveContinuation();

        limits.haveResult();
        assertTrue(limits.allowContinuation());
        limits.haveContinuation();

        assertTrue(limits.allowContinuation());
        limits.haveContinuation();

        assertFalse(limits.allowContinuation());
    }

    @Test
    public void testLimitEmptyContinuations() {
        MWApiLimits limits = new MWApiLimits(10, 10, 2);

        limits.haveResult();
        limits.haveContinuation();
        assertTrue(limits.allowContinuation());

        limits.haveContinuation();
        assertFalse(limits.allowContinuation());

        limits.haveResult();
        assertTrue(limits.allowContinuation());
    }

}
