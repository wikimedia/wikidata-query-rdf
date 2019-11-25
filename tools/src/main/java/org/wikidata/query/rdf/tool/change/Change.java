package org.wikidata.query.rdf.tool.change;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.OUTPUT_DATE_FORMATTER;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.tool.exception.RetryableException;

import com.google.common.collect.ImmutableList;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A change in an entity in Wikibase.
 */
@SuppressFBWarnings("FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY")
public class Change implements Comparable<Change> {
    /**
     * Change that is not associated with specific revision.
     */
    public static final long NO_REVISION = -1L;
    /**
     * Entity that changed.
     */
    private final String entityId;
    /**
     * Revision that the change changed to.
     */
    private final long revision;
    /**
     * Timestamp of the change.
     */
    private final Instant timestamp;
    /**
     * Chronology Id.
     */
    private final String chronologyId;

    /**
     * Set of processed statements for the change.
     */
    private Collection<Statement> statements;

    /**
     * Cleanup list for the change.
     */
    private Collection<String> refCleanupList = Collections.emptyList();

    /**
     * Cleanup list for the change.
     */
    private Collection<String> valueCleanupList = Collections.emptyList();

    /**
     * Offset of the change in the external stream.
     */
    private final long offset;

    public Change(String entityId, long revision, Instant timestamp, long offset) {
        this(entityId, revision, timestamp, offset, null);
    }

    public Change(String entityId, long revision, Instant timestamp, long offset, String chronologyId) {
        this.entityId = cleanEntityId(entityId);
        this.revision = revision;
        this.timestamp = timestamp;
        this.offset = offset;
        this.chronologyId = chronologyId;
    }

    private String cleanEntityId(String entityIdWithPrefix) {
        // FIXME: this should not be hardcoded
        if (entityIdWithPrefix.startsWith("Property:")) {
           return entityIdWithPrefix.substring("Property:".length());
        } else if (entityIdWithPrefix.startsWith("Item:")) {
            return entityIdWithPrefix.substring("Item:".length());
        } else if (entityIdWithPrefix.startsWith("Lexeme:")) {
            return entityIdWithPrefix.substring("Lexeme:".length());
        }
        return entityIdWithPrefix;
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
     * The offset of the change in the external stream.
     * Note that not all changes may come from an external stream,
     * and not all changes may come from the same stream.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of the change.
     *
     * @return the timestamp or null if that information is not available
     */
    public Instant timestamp() {
        return timestamp;
    }

    /**
     * The entity that changed.
     */
    public String chronologyId() {
        return chronologyId;
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
            b.append('@').append(OUTPUT_DATE_FORMATTER.format(timestamp));
            b.append('|').append(offset);
        }
        return b.toString();
    }

    /**
     * Detects changes. Implementations should store all state in subclasses of
     * Change.Batch.
     */
    public interface Source<B extends Change.Batch> extends Closeable {
        /**
         * Fetch the first batch.
         *
         * @throws RetryableException is the fetch fails in a retryable way
         */
        B firstBatch() throws RetryableException;

        /**
         * Fetches the next batch after lastBatch.
         *
         * @throws RetryableException is the fetch fails in a retryable way
         */
        B nextBatch(B lastBatch) throws RetryableException;

        /**
         * Called when the batch has been imported into the rdf store.
         */
        default void done(B batch) {
        }
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
         * Whether this batch had any changes, even invisible ones.
         * @return had changes?
         */
        boolean hasAnyChanges();

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
        String leftOffHuman();

        /**
         * Null or the latest date in the batch. If this is returned then the
         * updater process will attempt to mark the last update date in the rdf
         * store so it can pick up where it left off.
         */
        Instant leftOffDate();

        /**
         * Was this the last batch?
         */
        boolean last();

        /**
         * Simple default implementation of Batch.
         */
        abstract class AbstractDefaultImplementation implements Batch {
            /**
             * Changes in this batch.
             */
            private final ImmutableList<Change> changes;
            /**
             * How far did this batch advance?
             */
            private final long advanced;
            /**
             * Where did this batch leave off?
             */
            private final Object leftOff;

            public AbstractDefaultImplementation(ImmutableList<Change> changes, long advanced, Object leftOff) {
                this.changes = checkNotNull(changes);
                this.advanced = advanced;
                this.leftOff = leftOff;
            }

            @Override
            public List<Change> changes() {
                return changes;
            }

            @Override
            public boolean hasAnyChanges() {
                return !changes.isEmpty();
            }

            @Override
            public long advanced() {
                return advanced;
            }

            @Override
            public String leftOffHuman() {
                return leftOff.toString();
            }

            @Override
            public boolean last() {
                // By default we assume we're never done....
                return false;
            }
        }
    }

    @Override
    @SuppressFBWarnings(value = "EQ_COMPARETO_USE_OBJECT_EQUALS", justification = "This looks suspicious, but would need more investigation")
    // FIXME - since compareTo() is implemented, it would make sense to also implement equals() and hashCode(). But that
    // might lead to issues if the current code relies on Object.equals() in some places. It is probably simpler and
    // safer to move to an external comparator and might better represent the fact that comparing changes by offset is
    // only one of the way to compare them (e.g. natural ordering of Changes might also be by timestamp).
    public int compareTo(Change o) {
        return (int)(offset() - o.offset());
    }

    /**
     * Set statements collection.
     */
    public Collection<Statement> getStatements() {
        return statements;
    }

    /**
     * Return statements collection.
     */
    public void setStatements(Collection<Statement> statements) {
        this.statements = statements;
    }

    /**
     * Return value cleanup list.
     */
    public Collection<String> getValueCleanupList() {
        return valueCleanupList;
    }

    /**
     * Return reference cleanup list.
     */
    public Collection<String> getRefCleanupList() {
        return refCleanupList;
    }

    /**
     * Set reference cleanup list.
     */
    public void setRefCleanupList(Collection<String> cleanupList) {
        this.refCleanupList = cleanupList;
    }

    /**
     * Set value cleanup list.
     */
    public void setValueCleanupList(Collection<String> cleanupList) {
        this.valueCleanupList = cleanupList;
    }

    public class DelayedChange implements Delayed {
        /**
         * Delay for this change, if it needs to be retried.
         */
        private final Instant expires;

        DelayedChange(long delay) {
            this.expires = Instant.now().plusSeconds(delay);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(Duration.between(Instant.now(), expires).toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            DelayedChange other = (DelayedChange)o;
            return expires.compareTo(other.expires);
        }

        @Override
        public boolean equals(Object o) {
            return (o instanceof DelayedChange) && getChange() == ((DelayedChange) o).getChange();
        }

        @Override
        public int hashCode() {
            return getChange().hashCode();
        }

        public Change getChange() {
            return Change.this;
        }
    }

    public DelayedChange asDelayedChange(long timeout) {
        return new DelayedChange(timeout);
    }
}
