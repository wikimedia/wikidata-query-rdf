package org.wikidata.query.rdf.blazegraph.events;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class AsyncEventSender implements EventSender {
    private final BufferedEventSender bufferedEventGateSender;
    private final BufferedEventSender.Worker worker;
    private final EventSender underlyingSender;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @VisibleForTesting
    AsyncEventSender(BufferedEventSender bufferedEventGateSender,
                     BufferedEventSender.Worker worker, EventSender underlyingSender) {
        this.bufferedEventGateSender = bufferedEventGateSender;
        this.worker = worker;
        this.underlyingSender = underlyingSender;
    }

    public static AsyncEventSender wrap(int queueSize, int maxBatchSize, EventSender sender) {
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(queueSize);
        BufferedEventSender.Worker w = bufferedEventGateSender.newSendWorker(sender, maxBatchSize);
        w.start();
        return new AsyncEventSender(bufferedEventGateSender, w, sender);
    }

    @Override
    public void close() throws IOException {
        try {
            worker.stopAndWaitForCompletion();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for the executor service to shutdown, some events might be lost.");
            Thread.currentThread().interrupt();
        } finally {
            underlyingSender.close();
        }
    }

    @Override
    public boolean push(Event event) {
        return bufferedEventGateSender.push(event);
    }

    @Override
    public int push(Collection<Event> events) {
        return bufferedEventGateSender.push(events);
    }
}
