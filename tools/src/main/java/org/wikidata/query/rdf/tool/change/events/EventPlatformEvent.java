package org.wikidata.query.rdf.tool.change.events;

public interface EventPlatformEvent {
    EventsMeta meta();
    String schema();
}
