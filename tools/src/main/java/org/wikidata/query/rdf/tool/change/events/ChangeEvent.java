package org.wikidata.query.rdf.tool.change.events;

import java.time.Instant;

/**
 * Generic change event.
 * Every event that the event poller processes should implement this.
 * See https://github.com/wikimedia/mediawiki-event-schemas/blob/master/config/eventbus-topics.yaml
 * for the relationship between events and topics.
 */
public interface ChangeEvent extends EventPlatformEvent {
    /**
     * Page revision.
     */
    long revision();
    /**
     * Changed page title.
     */
    String title();
    /**
     * Changed page namespace.
     */
    long namespace();
    /**
     * Change timestamp.
     */
    Instant timestamp();
    /**
     * Domain for the event.
     */
    String domain();
}
