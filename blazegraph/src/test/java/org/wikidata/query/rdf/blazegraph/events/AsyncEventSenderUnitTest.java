package org.wikidata.query.rdf.blazegraph.events;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

public class AsyncEventSenderUnitTest {
    @Test
    public void testPush() throws InterruptedException {
        BufferedEventSender sender = new BufferedEventSender(10);
        List<Event> receivedEvents = new ArrayList<>();
        EventSender underlyingSender = receivedEvents::add;
        BufferedEventSender.Worker resender = sender.newSendWorker(receivedEvents::add, 10);
        List<Event> sentEvents = Stream.generate(EventTestUtils::newQueryEvent)
                .limit(10)
                .collect(toList());
        AsyncEventSender asyncSend = new AsyncEventSender(sender, resender, underlyingSender);
        asyncSend.push(sentEvents.get(0));
        asyncSend.push(sentEvents.subList(1, sentEvents.size()));
        // test in single thread mode, stop first and call run so that we fully drain the internal queue
        resender.stopAndWaitForCompletion();
        resender.run();
        assertThat(receivedEvents).isEqualTo(sentEvents);
    }

    @Test
    public void testClose() throws InterruptedException, IOException {
        BufferedEventSender bufferedSender = mock(BufferedEventSender.class);
        EventSender underlyingSender = mock(EventSender.class);
        BufferedEventSender.Worker resender = mock(BufferedEventSender.Worker.class);
        AsyncEventSender asyncSend = new AsyncEventSender(bufferedSender, resender, underlyingSender);
        asyncSend.close();
        verify(underlyingSender, times(1)).close();
        verify(resender, times(1)).stopAndWaitForCompletion();
    }
}
