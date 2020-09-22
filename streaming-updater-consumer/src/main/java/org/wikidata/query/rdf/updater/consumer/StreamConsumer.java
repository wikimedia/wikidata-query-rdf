package org.wikidata.query.rdf.updater.consumer;

import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;

import lombok.Value;

public interface StreamConsumer extends AutoCloseable {

    Batch poll(long timeout);

    void acknowledge();

    void close();

    @Value
    class Batch {
        ConsumerPatch patch;
    }
}
