package org.wikidata.query.rdf.tool.change;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.List;

import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.google.common.collect.ImmutableList;

/**
 * A change in an entity in Wikibase.
 */
public class Change {
    private final String entityId;
    private final long revision;
    private final Date timestamp;

    public Change(String entityId, long revision, Date timestamp) {
        if(entityId.startsWith("Property:")) {
            this.entityId = entityId.substring("Property:".length());
        } else {
            this.entityId = entityId;
        }
        this.revision = revision;
        this.timestamp = timestamp;
    }

    /**
     * The entity that changed.
     */
    public String entityId() {
        return entityId;
    }

    /**
     * The revision of the change.
     *
     * @return the revision number of -1 if that information is not available
     */
    public long revision() {
        return revision;
    }

    /**
     * The timestamp of the change.
     *
     * @return the timestamp or null if that information is not available
     */
    public Date timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        if (revision < -1 && timestamp == null) {
            return entityId;
        }
        StringBuilder b = new StringBuilder();
        b.append(entityId);
        if (revision >= 0) {
            b.append('@').append(revision);
        }
        if (timestamp != null) {
            b.append("@").append(timestamp);
        }
        return b.toString();
    }

    /**
     * Detects changes. Implementations should store all state in subclasses of
     * Change.Batch.
     */
    public interface Source<B extends Change.Batch> {
        /**
         * Fetch the first batch.
         */
        B firstBatch() throws RetryableException;

        /**
         * Fetches the next batch after lastBatch.
         */
        B nextBatch(B lastBatch) throws RetryableException;
    }

    /**
     * A batch of changes. Implementations should be immutable.
     */
    public interface Batch {
        /**
         * The changes in the batch.
         *
         * @return a list of changes. If the batch is empty then the list is
         *         empty. It is never null.
         */
        List<Change> changes();

        /**
         * The unit of advanced() in English. Used for logging.
         */
        String advancedUnits();

        /**
         * How much this batch is "worth" in units of advancedUnits(). Used for
         * logging.
         */
        long advanced();

        /**
         * Human readable version of where this batch ends. Used for logging. It
         * should be obvious how to continue from here if possible.
         */
        String upTo();

        /**
         * Was this the last batch?
         */
        boolean last();

        /**
         * Simple default implementation of Batch.
         */
        public static abstract class AbstractDefaultImplementation implements Batch {
            private final ImmutableList<Change> changes;
            private final long advanced;
            private final Object upTo;

            public AbstractDefaultImplementation(ImmutableList<Change> changes, long advanced, Object upTo) {
                this.changes = checkNotNull(changes);
                this.advanced = advanced;
                this.upTo = upTo;
            }

            @Override
            public List<Change> changes() {
                return changes;
            }

            @Override
            public long advanced() {
                return advanced;
            }

            @Override
            public String upTo() {
                return upTo.toString();
            }

            @Override
            public boolean last() {
                // By default we assume we're never done....
                return false;
            }
        }
    }
}
