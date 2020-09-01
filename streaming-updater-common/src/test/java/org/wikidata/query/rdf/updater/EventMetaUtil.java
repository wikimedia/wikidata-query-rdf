package org.wikidata.query.rdf.updater;

import java.time.Instant;
import java.util.UUID;

import org.wikidata.query.rdf.tool.change.events.EventsMeta;

/**
 * Provides factory methods for EventMeta for tests to use.
 *
 */
public final class EventMetaUtil {

    private EventMetaUtil() {
        // Utility class, should never be constructed
    }

    public static EventsMeta makeEventMeta() {
        return makeEventMeta(UUID.randomUUID(), UUID.randomUUID());
    }

    public static EventsMeta makeEventMeta(UUID eventUuid, UUID requestId) {
        Instant processingTime = Instant.EPOCH.plusMillis(1);
        return  new EventsMeta(processingTime, eventUuid.toString(), "unittest.local", "rdf_stream", requestId.toString());
    }
}
