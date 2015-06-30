package org.wikidata.query.rdf.tool.change;

import static java.lang.Math.min;

import java.util.Date;
import java.util.Locale;

import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.google.common.collect.ImmutableList;

/**
 * Blindly iterates an id range and returns those as "changes". Can be used to
 * load known ids.
 */
public class IdRangeChangeSource implements Change.Source<IdRangeChangeSource.Batch> {
    /**
     * Build and IdChangeSource for items as opposed to properties.
     */
    public static IdRangeChangeSource forItems(long start, long stop, long batchSize) {
        return new IdRangeChangeSource("Q%s", start, stop, batchSize);
    }

    /**
     * Format of the entity id.
     */
    private final String format;
    /**
     * First id to return.
     */
    private final long start;
    /**
     * Last id to return.
     */
    private final long stop;
    /**
     * Batch size to split up ids.
     */
    private final long batchSize;

    public IdRangeChangeSource(String format, long start, long stop, long batchSize) {
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

    /**
     * Batch implementation for this change source.
     */
    public final class Batch extends Change.Batch.AbstractDefaultImplementation {
        /**
         * Next id to start polling.
         */
        private final long nextStart;

        private Batch(ImmutableList<Change> changes, long advanced, long nextStart) {
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

        @Override
        public Date leftOffDate() {
            return null;
        }
    }

    /**
     * Build a batch starting at batchStart.
     */
    private Batch batch(long batchStart) {
        long batchStop = min(batchStart + batchSize, stop + 1);
        ImmutableList.Builder<Change> changes = ImmutableList.builder();
        for (long id = batchStart; id < batchStop; id++) {
            changes.add(new Change(String.format(Locale.ROOT, format, id), -1, null, id));
        }
        return new Batch(changes.build(), batchStop - batchStart, batchStop);
    }
}
