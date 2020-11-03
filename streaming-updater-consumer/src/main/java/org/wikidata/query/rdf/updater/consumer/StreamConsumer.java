package org.wikidata.query.rdf.updater.consumer;

import java.time.Duration;

import org.wikidata.query.rdf.tool.rdf.ConsumerPatch;

import lombok.Value;

public interface StreamConsumer extends AutoCloseable {

    Batch poll(Duration timeout);

    void acknowledge();

    void close();

    @Value
    class Batch {
        ConsumerPatch patch;
    }
}
