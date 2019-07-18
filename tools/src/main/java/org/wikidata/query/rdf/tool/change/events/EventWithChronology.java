package org.wikidata.query.rdf.tool.change.events;

/**
 * Event that has chronology_id field.
 */
public interface EventWithChronology {
    /**
     * Chronology ID.
     */
    String chronologyId();
}
