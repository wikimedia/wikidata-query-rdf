package org.wikidata.query.rdf.tool;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.wikidata.query.rdf.tool.change.Change;

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
        DeferredChanges deferredChanges = new DeferredChanges();
        Change delayedOK = new Change("Q567", 102, Instant.EPOCH, 102);
        Change delayedDup = new Change("Q123", 102, Instant.EPOCH, 102);
        Change delayedNotYet = new Change("Q890", 100, Instant.EPOCH, 100);


        deferredChanges.add(delayedOK, 0);
        deferredChanges.add(delayedDup, 0);
        deferredChanges.add(delayedNotYet, 1_000_000);

        Collection<Change> newChanges = deferredChanges.augmentWithDeferredChanges(originalChanges);
        // We expect the original Q123 and Q345, and Q567
        assertThat(newChanges).extracting(Change::entityId).containsExactly("Q123", "Q345", "Q567");
    }
}
