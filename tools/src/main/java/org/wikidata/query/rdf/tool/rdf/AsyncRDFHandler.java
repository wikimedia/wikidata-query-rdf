package org.wikidata.query.rdf.tool.rdf;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import com.google.common.annotations.VisibleForTesting;

@NotThreadSafe
public class AsyncRDFHandler implements RDFHandler {
    static final int STMT_BUFFER_SIZE = 100;
    private final RDFRecordedActionConsumer actionConsumer;
    private final RDFActionsReplayer replayer;
    private final boolean joinOnRDFEnd;
    private List<Statement> statementBuffer = new ArrayList<>(STMT_BUFFER_SIZE);

    @VisibleForTesting
    AsyncRDFHandler(RDFRecordedActionConsumer consumer, RDFActionsReplayer replayer, boolean joinOnRDFEnd) {
        actionConsumer = consumer;
        this.replayer = replayer;
        this.joinOnRDFEnd = joinOnRDFEnd;
    }

    /**
     * Builds a RDFHandle that replays asynchrously RDF events to the handler provided.
     * The returned handler is not thread-safe and should not receive events from different threads.
     */
    public static AsyncRDFHandler processAsync(RDFHandler handler, boolean joinOnRDFEnd, int bufferSize) {
        // Queue storing elements to replay to the handler given
        BlockingQueue<RDFRecordedAction> queue = new ArrayBlockingQueue<>(bufferSize);
        // Store the exception that caused the replayer to stop consuming the queue
        AtomicReference<Throwable> replayerException = new AtomicReference<>();

        // Lambda to check and rethrow the exception captured in replayerException
        // to the thread writing to this async RDF handler
        // Reason is to not block forever if the replayer has died.
        RDFRecordedActionConsumer consumer = (a) -> {
            try {
                do {
                    Throwable exc = replayerException.get();
                    if (exc != null) {
                        throw new RDFHandlerException("Queue dead cannot record", exc);
                    }
                } while (!queue.offer(a, 1, SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RDFHandlerException(e);
            }
        };

        RDFActionsReplayer replayer = new RDFActionsReplayer(queue, handler, replayerException::set);
        AsyncRDFHandler recorder = new AsyncRDFHandler(consumer, replayer, joinOnRDFEnd);
        replayer.setUncaughtExceptionHandler((t, e) -> replayerException.set(e));
        replayer.start();
        return recorder;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        recordAction(RDFHandler::startRDF);
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        recordAction(RDFHandler::endRDF);
        if (joinOnRDFEnd) {
            try {
                waitForCompletion();
            } catch (InterruptedException e) {
                throw new RDFHandlerException(e);
            }
        }
    }

    public void waitForCompletion() throws InterruptedException {
        replayer.waitForCompletion();
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        recordAction((r)  -> r.handleNamespace(prefix, uri));

    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        // Use a list of statement to reduce GC pressure by allocation a lambda per
        // statement
        statementBuffer.add(st);
        if (statementBuffer.size() >= STMT_BUFFER_SIZE) {
            flushStatementBuffer();
        }
    }

    private void flushStatementBuffer() throws RDFHandlerException {
        List<Statement> statements = statementBuffer;
        statementBuffer = new ArrayList<>(STMT_BUFFER_SIZE);
        actionConsumer.accept(r -> {
            for (Statement statement : statements) {
                r.handleStatement(statement);
            }
        });
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        recordAction(r -> r.handleComment(comment));
    }

    private void recordAction(RDFRecordedAction recordedAction) throws RDFHandlerException {
        if (!statementBuffer.isEmpty()) {
            flushStatementBuffer();
        }
        actionConsumer.accept(recordedAction);
    }

    @VisibleForTesting
    static class RDFActionsReplayer extends Thread {
        private final BlockingQueue<RDFRecordedAction> queue;
        private final RDFHandler targetHandler;
        private final Consumer<Throwable> onFailureListener;
        private volatile boolean end;

        RDFActionsReplayer(BlockingQueue<RDFRecordedAction> queue, RDFHandler targetHandler, Consumer<Throwable> onFailureListener) {
            super(RDFActionsReplayer.class.getName());
            this.queue = queue;
            this.targetHandler = targetHandler;
            this.onFailureListener = onFailureListener;
        }

        @Override
        public void run() {
            try {
                while (!end || !queue.isEmpty()) {
                    // Use poll with timeout because we need to re-check end from
                    // times to times
                    RDFRecordedAction action = queue.poll(100, MILLISECONDS);
                    if (action != null) {
                        action.exec(targetHandler);
                    }
                }
            } catch (RDFHandlerException e) {
                onFailureListener.accept(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                onFailureListener.accept(e);
            }
        }

        @VisibleForTesting
        void end() {
            this.end = true;
        }

        public void waitForCompletion() throws InterruptedException {
            end();
            join();
        }

        public boolean isRunning() {
            return super.isAlive();
        }
    }

    @FunctionalInterface
    interface RDFRecordedAction {
        void exec(RDFHandler targetHandler) throws RDFHandlerException;
    }

    @FunctionalInterface
    interface RDFRecordedActionConsumer {
        void accept(RDFRecordedAction action) throws RDFHandlerException;
    }
}
