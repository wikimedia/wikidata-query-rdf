package org.wikidata.query.rdf.tool.change;

import static java.lang.Math.min;

import java.util.Locale;

import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.google.common.collect.ImmutableList;

/**
 * Blindly iterates an id range and returns those as "changes". Can be used to
 * load known ids.
 */
public class IdChangeSource implements Change.Source<IdChangeSource.Batch> {
    public static IdChangeSource forItems(long start, long stop, long batchSize) {
        return new IdChangeSource("Q%s", start, stop, batchSize);
    }

    private final String format;
    private final long start;
    private final long stop;
    private final long batchSize;

    public IdChangeSource(String format, long start, long stop, long batchSize) {
        this.format = format;
        this.start = start;
        this.stop = stop;
        this.batchSize = batchSize;
    }

    @Override
    public Batch firstBatch() throws RetryableException {
        return batch(start);
    }

    @Override
    public Batch nextBatch(Batch lastBatch) throws RetryableException {
        return batch(lastBatch.nextStart);
    }

    public class Batch extends Change.Batch.AbstractDefaultImplementation {
        private final long nextStart;

        public Batch(ImmutableList<Change> changes, long advanced, long nextStart) {
            super(changes, advanced, nextStart - 1);
            this.nextStart = nextStart;
        }

        @Override
        public String advancedUnits() {
            return "ids";
        }

        @Override
        public boolean last() {
            return nextStart > stop;
        }
    }

    private Batch batch(long batchStart) {
        long batchStop = min(batchStart + batchSize, stop + 1);
        ImmutableList.Builder<Change> changes = ImmutableList.builder();
        for (long id = batchStart; id < batchStop; id++) {
            changes.add(new Change(String.format(Locale.ROOT, format, id), -1, null));
        }
        return new Batch(changes.build(), batchStop - batchStart, batchStop);
    }
}
