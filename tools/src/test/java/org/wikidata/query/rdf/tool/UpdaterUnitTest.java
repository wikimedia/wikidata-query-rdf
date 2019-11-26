package org.wikidata.query.rdf.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class UpdaterUnitTest {
    @Captor
    private ArgumentCaptor<Instant> lestOffDateCaptor;

    @Test
    public void testUpdateLeftOffTime() {
        Instant leftOffInstant1 = Instant.ofEpochMilli(25);
        Instant leftOffInstant2 = Instant.ofEpochSecond(40);
        ImmutableList<Change> changes = ImmutableList.of(
                new Change("Q2", 1, Instant.ofEpochSecond(10), 2),
                new Change("Q3", 2, Instant.ofEpochMilli(20), 3)
        );
        TestChange batch1 = new TestChange(changes, 20, leftOffInstant1, false);
        changes = ImmutableList.of(
                new Change("Q2", 1, Instant.ofEpochSecond(30), 4),
                new Change("Q3", 2, Instant.ofEpochMilli(40), 5)
        );
        TestChange batch2 = new TestChange(changes, 20, leftOffInstant2, true);

        TestChangeSource source = new TestChangeSource(Arrays.asList(batch1, batch2));
        WikibaseRepository wbRepo = mock(WikibaseRepository.class);
        RdfRepository rdfRepo = mock(RdfRepository.class);
        Munger munger = Munger.builder(UrisSchemeFactory.WIKIDATA).build();
        ExecutorService executorService = Executors.newFixedThreadPool(2, (r) -> new Thread(r, "Thread-" + this.getClass().getSimpleName()));
        Updater<TestChange> updater = new Updater<>(source, wbRepo, rdfRepo, munger,
                executorService, true, 100, UrisSchemeFactory.WIKIDATA, false, new MetricRegistry());
        updater.run();
        verify(rdfRepo, times(2)).updateLeftOffTime(lestOffDateCaptor.capture());
        assertThat(lestOffDateCaptor.getAllValues()).containsExactly(leftOffInstant1.minusSeconds(1), leftOffInstant2.minusSeconds(1));
        assertThat(source.isBatchMarkedDone(batch1)).isTrue();
        assertThat(source.isBatchMarkedDone(batch2)).isTrue();
    }

    public static class TestChangeSource implements Change.Source<TestChange> {
        private final List<TestChange> batches;
        private final Set<TestChange> doneBatches = new HashSet<>();
        private Iterator<TestChange> iterator;

        public TestChangeSource(List<TestChange> batches) {
            this.batches = batches;
        }

        @Override
        public TestChange firstBatch() throws RetryableException {
            iterator = batches.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }

        @Override
        public TestChange nextBatch(TestChange lastBatch) {
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        public void done(TestChange batch) {
            doneBatches.add(batch);
        }

        public boolean isBatchMarkedDone(TestChange batch) {
            return doneBatches.contains(batch);
        }

        @Override
        public void close() {}
    }

    public static class TestChange extends Change.Batch.AbstractDefaultImplementation {
        private final Instant lestOff;
        private final boolean last;

        public TestChange(ImmutableList<Change> changes, long advanced, Instant leftOff, boolean isLast) {
            super(changes, advanced, leftOff);
            this.lestOff = leftOff;
            this.last = isLast;
        }

        @Override
        public String advancedUnits() {
            return "milliseconds";
        }

        @Override
        public Instant leftOffDate() {
            return lestOff;
        }

        @Override
        public boolean last() {
            return last;
        }
    }
}
