package org.wikidata.query.rdf.blazegraph.events;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EventFileSenderUnitTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private EventFileSender sut;
    private Path path;
    private final IntFunction<String> ID_MAPPER = i -> "id_" + i;

    @Before
    public void before() {
        path = Paths.get(temporaryFolder.getRoot() + "/event.log");
        sut = new EventFileSender(path);
    }

    @Test
    public void testFileSingleEvent() throws IOException {
        Event sentEvent = EventTestUtils.newQueryEvent();
        boolean pushed = sut.push(sentEvent);
        assertThat(pushed).isTrue();
        List<String> receivedEvents = Files.readAllLines(path);

        assertThat(receivedEvents).hasSize(1);

        assertThat(receivedEvents.get(0)).isEqualTo(EventTestUtils.queryEventJsonString());
    }

    @Test
    public void testFileMultipleEvents() throws IOException {
        int count = 10;

        List<Event> sentEvents = generateEventsList(count);
        int pushed = sut.push(sentEvents);
        assertThat(pushed).isEqualTo(sentEvents.size());
        List<String> receivedEvents = Files.readAllLines(path);

        List<String> jsonEvents = generateJsonEventsList(count);
        assertThat(receivedEvents).containsExactlyElementsOf(jsonEvents);
    }

    @Test
    public void testAppendingEvents() throws IOException {
        List<Event> sentEvents = generateEventsList(30);
        int pushed = sut.push(sentEvents.subList(0, 10));
        assertThat(pushed).isEqualTo(10);
        sut = new EventFileSender(path);
        pushed = sut.push(sentEvents.subList(10, 20));
        assertThat(pushed).isEqualTo(10);
        sut.push(sentEvents.subList(20, 30));
        List<String> receivedEvents = Files.readAllLines(path);
        List<String> jsonEvents = generateJsonEventsList(20);
        assertThat(receivedEvents).containsAll(jsonEvents);
    }

    private List<String> generateJsonEventsList(int count) {
        return generateById(count, EventTestUtils::queryEventJsonString);
    }

    private List<Event> generateEventsList(int count) {
        return generateById(count, EventTestUtils::newQueryEvent);
    }

    private <T> List<T> generateById(int count, Function<String, T> newQueryEvent) {
        return IntStream.range(1, count + 1)
                .mapToObj(ID_MAPPER)
                .map(newQueryEvent)
                .collect(toList());
    }
}
