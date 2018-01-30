package org.wikidata.query.rdf.tool.change.events;

import java.util.Date;

/**
 * Generic change event.
 * Every event that the event poller processes should implement this.
 * See https://github.com/wikimedia/mediawiki-event-schemas/blob/master/config/eventbus-topics.yaml
 * for the relationship between events and topics.
 */
public interface ChangeEvent {
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
    Date timestamp();
    /**
     * Domain for the event.
     */
    String domain();
    /**
     * This change is redundant and can be skipped.
     */
    boolean isRedundant();
}
