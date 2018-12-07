package org.wikidata.query.rdf.tool;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.DelayQueue;

import org.junit.Test;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.Change.DelayedChange;

import com.google.common.collect.ImmutableList;

/**
 * Test deferred changes merging function on Updater.
 */
public class DeferredChangesUnitTest {

    @Test
    public void deferredUpdates() {
        List<Change> originalChanges = ImmutableList.of(
                new Change("Q123", 100, Instant.EPOCH, 100),
                new Change("Q345", 101, Instant.EPOCH, 101)
        );
        DelayQueue<DelayedChange> delayedChanges = new DelayQueue<>();
        Change delayedOK = new Change("Q567", 102, Instant.EPOCH, 102);
        Change delayedDup = new Change("Q123", 102, Instant.EPOCH, 102);
        Change delayedNotYet = new Change("Q890", 100, Instant.EPOCH, 100);
        delayedOK.delay(delayedChanges, 0);
        delayedDup.delay(delayedChanges, 0);
        delayedNotYet.delay(delayedChanges, 1_000_000);

        Collection<Change> newChanges = Updater.addDeferredChanges(delayedChanges, originalChanges);
        // We expect the original Q123 and Q345, and Q567
        assertThat(newChanges).extracting(Change::entityId).containsExactly("Q123", "Q345", "Q567");
    }

}
