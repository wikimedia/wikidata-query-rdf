package org.wikidata.query.rdf.tool;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.DelayQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.change.Change;

/**
 * A class to hold "deferred changed". This behavior was originally introduced to
 * circumvent lag when fetching the entity data from wikibase.
 * See https://phabricator.wikimedia.org/T210901 for more context.
 */
public class DeferredChanges {
    private final DelayQueue<Change.DelayedChange> queue = new DelayQueue<>();
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * If there are any deferred changes, add them to changes list.
     * @return Modified collection as iterable.
     */
    public Collection<Change> augmentWithDeferredChanges(Collection<Change> newChanges) {
        if (queue.isEmpty()) {
            return newChanges;
        }
        List<Change> allChanges = new LinkedList<>(newChanges);
        int deferrals = 0;
        Set<String> changeIds = newChanges.stream().map(Change::entityId).collect(toImmutableSet());
        for (Change.DelayedChange deferred = queue.poll();
             deferred != null;
             deferred = queue.poll()) {
            if (changeIds.contains(deferred.getChange().entityId())) {
                // This title ID already has newer change, drop the deferral.
                // NOTE: here we assume that incoming stream always has changes in order of increasing revisions,
                // which sounds plausible but not guaranteed.
                continue;
            }
            allChanges.add(deferred.getChange());
            deferrals++;
        }
        log.info("Added {} deferred changes, {} still in the queue", deferrals, queue.size());
        return allChanges;
    }

    public void add(Change change, long timeout) {
        queue.add(change.asDelayedChange(timeout));
    }
}
