package org.wikidata.query.rdf.tool.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.test.StatementHelper.statements;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.wikidata.query.rdf.tool.rdf.RDFPatch;
import org.wikidata.query.rdf.tool.rdf.RDFPatchResult;
import org.wikidata.query.rdf.tool.rdf.RdfRepositoryUpdater;

import com.codahale.metrics.MetricRegistry;

@RunWith(MockitoJUnitRunner.class)
public class StreamingUpdaterUnitTest {
    @Mock
    StreamConsumer consumer;
    @Mock
    RdfRepositoryUpdater rdfRepositoryUpdater;

    @Test
    public void test() throws InterruptedException {
        RDFPatch patch = new RDFPatch(statements(), statements(), statements(), statements());
        RDFPatchResult rdfPatchResult = new RDFPatchResult(2, 1, 2, 1);
        LongAdder patchApplied = new LongAdder();
        CountDownLatch countdown = new CountDownLatch(5);
        when(consumer.poll(anyLong())).thenAnswer((Answer<StreamConsumer.Batch>) invocationOnMock -> new StreamConsumer.Batch(patch));
        when(rdfRepositoryUpdater.applyPatch(any())).thenAnswer((Answer<RDFPatchResult>) i -> {
            countdown.countDown();
            patchApplied.increment();
            return rdfPatchResult;
        });
        MetricRegistry registry = new MetricRegistry();
        StreamingUpdater updater = new StreamingUpdater(consumer, rdfRepositoryUpdater, registry);
        Thread t = new Thread(updater);
        t.start();
        // Wait for five patches to be applied and stop the updater

        countdown.await();
        updater.close();
        t.join();
        // Make sure that we called the methods the right number of times
        // This updater does not much other than bridging a consumer and a repository
        verify(consumer, times(patchApplied.intValue())).poll(anyLong());
        verify(consumer, times(patchApplied.intValue())).acknowledge();
        verify(consumer, times(1)).close();
        verify(rdfRepositoryUpdater, times(patchApplied.intValue())).applyPatch(same(patch));
        verify(rdfRepositoryUpdater, times(1)).close();

        assertThat(registry.counter("mutations").getCount()).isEqualTo(patchApplied.intValue());
        assertThat(registry.counter("divergences").getCount()).isEqualTo(patchApplied.intValue());
        assertThat(registry.counter("shared-element-mutations").getCount()).isEqualTo(patchApplied.intValue());
        assertThat(registry.counter("shared-element-redundant-mutations").getCount()).isEqualTo(patchApplied.intValue());
    }
}
