package org.wikidata.query.rdf.tool.stream;

import org.wikidata.query.rdf.tool.rdf.Patch;

import lombok.Value;

public interface StreamConsumer extends AutoCloseable {

    Batch poll(long timeout);

    void acknowledge();

    void close();

    @Value
    class Batch {
        Patch patch;
    }
}
