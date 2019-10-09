package org.wikidata.query.rdf.blazegraph.events;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BufferedEventSender implements EventSender {
    private final BlockingQueue<Event> queue;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    BufferedEventSender(int maxCap) {
        this.queue = new ArrayBlockingQueue<>(maxCap);
    }

    @Override
    public boolean push(Event event) {
        return offer(event);
    }

    private boolean offer(Event event) {
        if (!queue.offer(event)) {
            log.error("Cannot buffer event {}, queue full", event.getMetadata().getStream());
            return false;
        }
        return true;
    }

    Worker newSendWorker(EventSender sender, int bufferSize) {
        return new Worker(sender, queue, bufferSize);
    }

    static class Worker extends Thread {
        private volatile boolean run = true;
        private final EventSender sender;
        private final int bufferSize;
        private final BlockingQueue<Event> queue;

        Worker(EventSender sender, BlockingQueue queue, int bufferSize) {
            super(Worker.class.getName() + "-" + sender.getClass().getSimpleName());
            this.setDaemon(true);
            this.sender = sender;
            this.queue = queue;
            this.bufferSize = bufferSize;
        }

        /**
         * Tell the thread to exist its run loop after the queue has been drained. Wait for the thread
         * completion if it's running.
         * Calling this method before starting the thread will cause the run method to immediately exit after having
         * drained the queue.
         * @throws InterruptedException
         */
        void stopAndWaitForCompletion() throws InterruptedException {
            this.run = false;
            join(2000);
            if (isAlive()) {
                this.interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (run || !queue.isEmpty()) {
                    Event event = queue.poll(100, MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                    List<Event> events = new ArrayList<>(bufferSize);
                    events.add(event);
                    queue.drainTo(events, bufferSize - 1);
                    this.sender.push(events);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
