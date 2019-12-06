package org.wikidata.query.rdf.tool.rdf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.wikidata.query.rdf.test.StatementHelper;

public class AsyncRDFHandlerUnitTest {
    @Test
    public void testRecordingToTheQueue() throws RDFHandlerException, InterruptedException {
        RDFHandler mockHandler = mock(RDFHandler.class);
        AsyncRDFHandler.RDFActionsReplayer replayer = mock(AsyncRDFHandler.RDFActionsReplayer.class);
        when(replayer.isRunning()).thenReturn(true);
        Queue<AsyncRDFHandler.RDFRecordedAction> queue = new LinkedList<>();
        AsyncRDFHandler handler = new AsyncRDFHandler(queue::add, replayer, false);
        // msg 1
        handler.startRDF();
        // msg 2
        handler.handleComment("this is a comment");
        // msg 3
        handler.handleNamespace("pref", "my_prefix");
        // msg 4
        for (int i = 0; i < AsyncRDFHandler.STMT_BUFFER_SIZE; i++) {
            handler.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));
        }
        // msg 5
        handler.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));
        // msg 6
        handler.endRDF();

        assertThat(queue.size()).isEqualTo(6);

        AsyncRDFHandler.RDFRecordedAction action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler).startRDF();

        action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler).handleComment("this is a comment");

        action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler).handleNamespace("pref", "my_prefix");

        action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler, times(AsyncRDFHandler.STMT_BUFFER_SIZE)).handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));

        action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        // called one more time
        verify(mockHandler, times(AsyncRDFHandler.STMT_BUFFER_SIZE + 1)).handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));

        action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler).endRDF();

        verify(replayer, never()).waitForCompletion();

        assertThat(queue).isEmpty();
    }

    @Test
    public void testJoinOnRDFEnd() throws RDFHandlerException, InterruptedException {
        RDFHandler mockHandler = mock(RDFHandler.class);
        AsyncRDFHandler.RDFActionsReplayer replayer = mock(AsyncRDFHandler.RDFActionsReplayer.class);
        when(replayer.isRunning()).thenReturn(true);
        Queue<AsyncRDFHandler.RDFRecordedAction> queue = new LinkedList<>();
        AsyncRDFHandler handler = new AsyncRDFHandler(queue::add, replayer, true);
        handler.endRDF();
        verify(replayer).waitForCompletion();

        AsyncRDFHandler.RDFRecordedAction action = queue.poll();
        assertThat(action).isNotNull();
        action.exec(mockHandler);
        verify(mockHandler).endRDF();
    }

    @Test
    public void testReplayer() throws RDFHandlerException {
        BlockingQueue<AsyncRDFHandler.RDFRecordedAction> queue = new ArrayBlockingQueue<>(100);
        queue.add(RDFHandler::startRDF);
        queue.add(RDFHandler::endRDF);
        RDFHandler mockHandler = mock(RDFHandler.class);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AsyncRDFHandler.RDFActionsReplayer replayer = new AsyncRDFHandler.RDFActionsReplayer(queue, mockHandler, exception::set);
        // Force run synchronously for the sake of the test, this also tests that we fully drain the queue before quitting.
        replayer.end();
        replayer.run();
        verify(mockHandler).startRDF();
        verify(mockHandler).endRDF();
        assertThat(exception.get()).isNull();
    }

    @Test
    public void testWaitOnCompletion() throws InterruptedException, RDFHandlerException {
        BlockingQueue<AsyncRDFHandler.RDFRecordedAction> queue = new ArrayBlockingQueue<>(100);
        RDFHandler handler = mock(RDFHandler.class);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AsyncRDFHandler.RDFActionsReplayer replayer = new AsyncRDFHandler.RDFActionsReplayer(queue, handler, exception::set);
        replayer.start();
        assertThat(replayer.isRunning()).isTrue();
        queue.add((r) -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new RDFHandlerException(e);
            }
            r.startRDF();
        });
        queue.add((r) -> {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new RDFHandlerException(e);
            }
            r.endRDF();
        });
        replayer.waitForCompletion();
        // Make sure that the queue is freed and that we replayed
        verify(handler).startRDF();
        verify(handler).endRDF();
        assertThat(exception.get()).isNull();
        assertThat(queue).isEmpty();
        assertThat(replayer.isRunning()).isFalse();
    }

    @Test(timeout = 1000L)
    public void testWaitOnCompletionDoesBlockForEver() throws InterruptedException {
        BlockingQueue<AsyncRDFHandler.RDFRecordedAction> queue = new ArrayBlockingQueue<>(100);
        RDFHandler handler = mock(RDFHandler.class);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AsyncRDFHandler.RDFActionsReplayer replayer = new AsyncRDFHandler.RDFActionsReplayer(queue, handler, exception::set);
        replayer.start();
        assertThat(replayer.isRunning()).isTrue();
        // We tell the replayer to exit its loop while certainly polling on an empty queue
        // (make sure we do not poll forever without rechecking that we have to exit)
        replayer.end();
        replayer.waitForCompletion();
    }

    @Test
    public void testExceptionCaptured() throws InterruptedException {
        BlockingQueue<AsyncRDFHandler.RDFRecordedAction> queue = new ArrayBlockingQueue<>(100);
        RDFHandler handler = mock(RDFHandler.class);
        AtomicReference<Throwable> exception = new AtomicReference<>();
        AsyncRDFHandler.RDFActionsReplayer replayer = new AsyncRDFHandler.RDFActionsReplayer(queue, handler, exception::set);
        replayer.start();
        assertThat(replayer.isRunning()).isTrue();
        queue.add((r) -> {
            throw new RDFHandlerException("");
        });
        replayer.waitForCompletion();
        assertThat(exception.get()).isInstanceOf(RDFHandlerException.class);
    }

    @Test
    public void asyncTest() throws RDFHandlerException, InterruptedException {
        RDFHandler targetHandler = mock(RDFHandler.class);
        AsyncRDFHandler asyncRDFHandler = AsyncRDFHandler.processAsync(targetHandler, false, 100);

        asyncRDFHandler.startRDF();
        asyncRDFHandler.handleComment("this is a comment");
        asyncRDFHandler.handleNamespace("pref", "my_prefix");
        for (int i = 0; i < AsyncRDFHandler.STMT_BUFFER_SIZE; i++) {
            asyncRDFHandler.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));
        }
        asyncRDFHandler.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));
        asyncRDFHandler.endRDF();

        asyncRDFHandler.waitForCompletion();

        verify(targetHandler).startRDF();
        verify(targetHandler).handleComment("this is a comment");
        verify(targetHandler).handleNamespace("pref", "my_prefix");
        verify(targetHandler, times(AsyncRDFHandler.STMT_BUFFER_SIZE + 1)).handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o"));
        verify(targetHandler).endRDF();
    }

    @Test(timeout = 5000)
    public void testExceptionIsPropagated() throws InterruptedException, RDFHandlerException {
        assertExceptionIsPropagated(RDFHandler::startRDF, new RDFHandlerException("boom"));
        assertExceptionIsPropagated(RDFHandler::endRDF, new RDFHandlerException("boom"));
        assertExceptionIsPropagated(r -> r.handleComment("com"), new RDFHandlerException("boom"));
        assertExceptionIsPropagated(r -> r.handleNamespace("pref", "pref"), new RDFHandlerException("boom"));
        assertExceptionIsPropagated(r -> r.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o")), new RDFHandlerException("boom"));

        assertExceptionIsPropagated(RDFHandler::startRDF, new RuntimeException("boom"));
        assertExceptionIsPropagated(RDFHandler::endRDF, new RuntimeException("boom"));
        assertExceptionIsPropagated(r -> r.handleComment("com"), new RuntimeException("boom"));
        assertExceptionIsPropagated(r -> r.handleNamespace("pref", "pref"), new RuntimeException("boom"));
        assertExceptionIsPropagated(r -> r.handleStatement(StatementHelper.statement("pref:s", "pref:p", "pref:o")), new RuntimeException("boom"));
    }

    public <E extends Throwable> void assertExceptionIsPropagated(AsyncRDFHandler.RDFRecordedAction action,
            E exc) throws RDFHandlerException, InterruptedException {
        RDFHandler targetHandler = mock(RDFHandler.class);
        AsyncRDFHandler asyncRDFHandler = AsyncRDFHandler.processAsync(targetHandler, false, 100);
        action.exec(doThrow(exc).when(targetHandler));
        action.exec(asyncRDFHandler);
        // Since we run async the exception will be propagated on the
        // next call after the targetHandler will actually receive our message
        // We have no way to control this so we keep sending message until we hit
        // this exception.
        while (true) {
            try {
                action.exec(asyncRDFHandler);
            } catch (RDFHandlerException t) {
                assertThat(t.getCause()).isInstanceOf(exc.getClass());
                assertThat(exc.getMessage()).isEqualTo(t.getCause().getMessage());
                return;
            }
            Thread.sleep(1L);
        }
    }
}
