package org.wikidata.query.rdf.blazegraph.events;

import java.util.Collection;

public interface EventSender {
    boolean push(Event event);

    /**
     * @return the number of events properly sent before the first failure if any
     */
    default int push(Collection<Event> events) {
        int nb = 0;
        for (Event e : events) {
            if (!push(e)) {
                return nb;
            }
            nb++;
        }
        return nb;
    }

    default void close() {}
}
