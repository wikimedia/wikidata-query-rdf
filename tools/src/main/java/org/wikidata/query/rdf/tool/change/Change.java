package org.wikidata.query.rdf.tool.change;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.google.common.collect.ImmutableList;

/**
 * A change in an entity in Wikibase.
 */
public class Change implements Comparable<Change> {
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
    private final Date timestamp;

    /**
     * Set of processed statements for the change.
     */
    private Collection<Statement> statements;

    /**
     * Cleanup list for the change.
     */
    private Collection<String> cleanupList;

    /**
     * rcid of the change.
     */
    private final long rcid;

    public Change(String entityId, long revision, Date timestamp, long rcid) {
        if (entityId.startsWith("Property:")) {
            this.entityId = entityId.substring("Property:".length());
        } else if (entityId.startsWith("Item:")) {
            this.entityId = entityId.substring("Item:".length());
        } else {
            this.entityId = entityId;
        }
        this.revision = revision;
        this.timestamp = timestamp;
        this.rcid = rcid;
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
     * The rcid of the change.
     *
     * @return the rcid number
     */
    public long rcid() {
        return rcid;
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
            b.append("@").append(WikibaseRepository.outputDateFormat().format(timestamp));
            b.append("|").append(rcid);
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
        String leftOffHuman();

        /**
         * Null or the latest date in the batch. If this is returned then the
         * updater process will attempt to mark the last update date in the rdf
         * store so it can pick up where it left off.
         */
        Date leftOffDate();

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
    public int compareTo(Change o) {
        return (int)(rcid() - o.rcid());
    }

    /**
     * Set statements collection.
     * @return
     */
    public Collection<Statement> getStatements() {
        return statements;
    }

    /**
     * Return statements collection.
     * @return
     */
    public void setStatements(Collection<Statement> statements) {
        this.statements = statements;
    }

    /**
     * Set cleanup list.
     * @return
     */
    public Collection<String> getCleanupList() {
        return cleanupList;
    }

    /**
     * Return cleanup list.
     * @param cleanupList
     */
    public void setCleanupList(Collection<String> cleanupList) {
        this.cleanupList = cleanupList;
    }
}
