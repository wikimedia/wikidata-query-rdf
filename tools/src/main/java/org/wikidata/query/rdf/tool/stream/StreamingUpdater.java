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
            // Use a custom flag instead of the interrupt flag because we want
            // to maximize our chances to properly cleanup our resources:
            // mainly close and commit pending kafka offsets and limit dup delivery
            // on restarts.
            // using interrupt() might mark some of the underlying IO resources
            // as unavailable preventing offsets to be committed.
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
