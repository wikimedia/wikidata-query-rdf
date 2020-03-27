package org.wikidata.query.rdf.blazegraph.events;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BufferedEventSenderUnitTest {
    @Captor
    private ArgumentCaptor<Collection<Event>> captor;

    @Test
    public void testPushSingle() throws InterruptedException {
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(1);
        Event e = EventTestUtils.newQueryEvent();
        assertThat(bufferedEventGateSender.push(e)).isTrue();
        EventSender resendGate = mock(EventSender.class);

        BufferedEventSender.Worker resender = bufferedEventGateSender.newSendWorker(resendGate, 1);
        // Stop before so that we drain the queue without looping for ever
        resender.stopAndWaitForCompletion();
        resender.run();
        verify(resendGate, times(1)).push(captor.capture());
        assertThat(captor.getValue()).containsExactly(e);
    }

    @Test
    public void testPushReturnsFalseWhenFull() {
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(1);
        Event e = EventTestUtils.newQueryEvent();
        assertThat(bufferedEventGateSender.push(e)).isTrue();
        assertThat(bufferedEventGateSender.push(e)).isFalse();
        assertThat(bufferedEventGateSender.push(Collections.singleton(e))).isEqualTo(0);
    }

    @Test
    public void testBufferingOnResend() throws InterruptedException {
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(10);
        List<Event> events = Stream.generate(EventTestUtils::newQueryEvent).limit(10).collect(toList());
        Event[] batch1 = events.subList(0, 5).toArray(new Event[0]);
        Event[] batch2 = events.subList(5, 10).toArray(new Event[0]);
        assertThat(bufferedEventGateSender.push(events)).isEqualTo(10);
        EventSender resendGate = mock(EventSender.class);

        BufferedEventSender.Worker resender = bufferedEventGateSender.newSendWorker(resendGate, 5);
        // Stop before so that we drain the queue without looping for ever
        resender.stopAndWaitForCompletion();
        resender.run();
        verify(resendGate, times(2)).push(captor.capture());
        assertThat(captor.getAllValues()).size().isEqualTo(2);

        assertThat(captor.getAllValues().get(0)).containsExactly(batch1).size().isEqualTo(5);
        assertThat(captor.getAllValues().get(1)).containsExactly(batch2).size().isEqualTo(5);
    }

    @Test(timeout = 3000L)
    public void testConcurrency() throws InterruptedException {
        // More of an integration test to test that the events are not leaked when closing the resenders.
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(10);
        List<Event> events1 = Collections.synchronizedList(new ArrayList<>());
        List<Event> events2 = Collections.synchronizedList(new ArrayList<>());
        BufferedEventSender.Worker resender1 = bufferedEventGateSender.newSendWorker(events1::add, 5);
        BufferedEventSender.Worker resender2 = bufferedEventGateSender.newSendWorker(events2::add, 5);
        resender1.start();
        resender2.start();
        int nbEvents = 0;

        // Wait a bit so that we test that the resender loop does
        // properly handle the case when poll returns false (empty queue).
        // Ensuring this with a sleep is a bit weak but making this assertion properly would
        // require to expose more states or to mock the internal BlockingQueue.
        Thread.sleep(1000);
        Random r = new Random(123);
        Supplier<Collection<Event>> batchGenerator = () -> Stream.generate(EventTestUtils::newQueryEvent)
                .limit(r.nextInt(5))
                .collect(Collectors.toList());

        for (int i = 0; i < 10; i++) {
            Collection<Event> events = batchGenerator.get();
            nbEvents += events.size();

            assertThat(bufferedEventGateSender.push(events)).isEqualTo(events.size());
            while ((events1.size() + events2.size()) < nbEvents) {
                Thread.sleep(10);
            }
        }
        Collection<Event> events = batchGenerator.get();
        nbEvents += events.size();
        assertThat(bufferedEventGateSender.push(events)).isEqualTo(events.size());
        resender1.stopAndWaitForCompletion();
        resender2.stopAndWaitForCompletion();
        assertThat(events1.size() + events2.size()).isEqualTo(nbEvents);
    }

    @Test(timeout = 1000L)
    public void testResendInterruption() throws InterruptedException {
        BufferedEventSender bufferedEventGateSender = new BufferedEventSender(1);

        BufferedEventSender.Worker resender = bufferedEventGateSender.newSendWorker(mock(EventSender.class), 10);
        resender.start();
        resender.interrupt();
        int maxWait = 10;
        while (resender.isAlive() && maxWait-- > 0) {
            Thread.sleep(10);
        }
        assertThat(resender.isAlive()).isFalse();
    }
}
