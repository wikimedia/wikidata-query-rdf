package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;

import org.junit.Test;
import org.wikidata.query.rdf.blazegraph.AbstractRandomizedBlazegraphTestBase;
import org.wikidata.query.rdf.test.SystemPropertyContext;

import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.codahale.metrics.Timer;

public class MWApiServiceFactoryUnitTest extends AbstractRandomizedBlazegraphTestBase {

    @Test
    public void testGetLimitsFromParams_default() throws IOException {
        try (
            Closeable maxResultsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_RESULTS, null);
            Closeable maxContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_CONTINUATIONS, null);
            Closeable maxEmptyContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_EMPTY_CONTINUATIONS, null);
        ) {
            MWApiServiceFactory factory = new MWApiServiceFactory(new ServiceConfig(), new Timer());
            ServiceParams serviceParams = new ServiceParams();

            MWApiLimits limits = factory.getLimitsFromParams(serviceParams);

            assertEquals(10000, limits.limitResults);
            assertEquals(1000, limits.limitContinuations);
            assertEquals(25, limits.limitEmptyContinuations);
        }
    }

    @Test
    public void testGetLimitsFromParams_nonstandardProperties() throws IOException {
        try (
            Closeable maxResultsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_RESULTS, "100");
            Closeable maxContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_CONTINUATIONS, "75");
            Closeable maxEmptyContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_EMPTY_CONTINUATIONS, "10");
        ) {
            MWApiServiceFactory factory = new MWApiServiceFactory(new ServiceConfig(), new Timer());
            ServiceParams serviceParams = new ServiceParams();

            MWApiLimits limits = factory.getLimitsFromParams(serviceParams);

            assertEquals(100, limits.limitResults);
            assertEquals(75, limits.limitContinuations);
            assertEquals(10, limits.limitEmptyContinuations);
        }
    }

    @Test
    public void testGetLimitsFromParams_customParams() throws IOException {
        try (
            Closeable maxResultsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_RESULTS, null);
            Closeable maxContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_CONTINUATIONS, null);
            Closeable maxEmptyContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_EMPTY_CONTINUATIONS, null);
        ) {
            MWApiServiceFactory factory = new MWApiServiceFactory(new ServiceConfig(), new Timer());

            ServiceParams serviceParams = new ServiceParams();
            serviceParams.set(MWApiServiceFactory.LIMIT_RESULTS_KEY, createConstant("100"));
            serviceParams.set(MWApiServiceFactory.LIMIT_CONTINUATIONS_KEY, createConstant("75"));
            serviceParams.set(MWApiServiceFactory.LIMIT_EMPTY_CONTINUATIONS_KEY, createConstant("10"));

            MWApiLimits limits = factory.getLimitsFromParams(serviceParams);

            assertEquals(100, limits.limitResults);
            assertEquals(75, limits.limitContinuations);
            assertEquals(10, limits.limitEmptyContinuations);
        }
    }

    @Test
    public void testGetLimitsFromParams_backwardsCompatiblityParams() throws IOException {
        try (
            Closeable maxResultsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_RESULTS, null);
            Closeable maxContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_CONTINUATIONS, null);
            Closeable maxEmptyContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_EMPTY_CONTINUATIONS, null);
        ) {
            MWApiServiceFactory factory = new MWApiServiceFactory(new ServiceConfig(), new Timer());

            ServiceParams serviceParams = new ServiceParams();
            serviceParams.set(MWApiServiceFactory.LIMIT_RESULTS_KEY, createConstant("once"));

            MWApiLimits limits = factory.getLimitsFromParams(serviceParams);

            assertEquals(10000, limits.limitResults);
            assertEquals(1, limits.limitContinuations);
            assertEquals(25, limits.limitEmptyContinuations);
        }
    }

    @Test
    public void testGetLimitsFromParams_nonstandardProperties_customParams() throws IOException {
        try (
            Closeable maxResultsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_RESULTS, "7500");
            Closeable maxContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_CONTINUATIONS, "50");
            Closeable maxEmptyContinuationsContext = SystemPropertyContext.setProperty(MWApiServiceFactory.CONFIG_MAX_EMPTY_CONTINUATIONS, "10");
        ) {
            MWApiServiceFactory factory = new MWApiServiceFactory(new ServiceConfig(), new Timer());

            ServiceParams serviceParams = new ServiceParams();
            serviceParams.set(MWApiServiceFactory.LIMIT_RESULTS_KEY, createConstant("9000"));
            serviceParams.set(MWApiServiceFactory.LIMIT_CONTINUATIONS_KEY, createConstant("75"));
            serviceParams.set(MWApiServiceFactory.LIMIT_EMPTY_CONTINUATIONS_KEY, createConstant("20"));

            MWApiLimits limits = factory.getLimitsFromParams(serviceParams);

            assertEquals(7500, limits.limitResults);
            assertEquals(50, limits.limitContinuations);
            assertEquals(10, limits.limitEmptyContinuations);
        }
    }

}
