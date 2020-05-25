package org.wikidata.query.rdf.tool.stream;

import org.wikidata.query.rdf.tool.rdf.RdfRepositoryUpdater;

public class StreamingUpdater implements Runnable {
    private final StreamConsumer consumer;
    private final RdfRepositoryUpdater repository;
    private volatile boolean stop;

    public StreamingUpdater(StreamConsumer consumer, RdfRepositoryUpdater repository) {
        this.consumer = consumer;
        this.repository = repository;
    }

    public void run() {
        try {
            while (!stop) {
                StreamConsumer.Batch b = consumer.poll(100);
                if (b == null) {
                    continue;
                }
                repository.applyPatch(b.getPatch());
                consumer.acknowledge();
            }
        } finally {
            try {
                consumer.close();
            } finally {
                repository.close();
            }
        }
    }

    public void close() {
        this.stop = true;
    }

}
