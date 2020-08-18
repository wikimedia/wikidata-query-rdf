package org.wikidata.query.rdf.tool.eventsummmary;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.options.OptionsUtils;

import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;

import de.thetaphi.forbiddenapis.SuppressForbidden;


public final class Summarizer {
    public static void main(String[] args) throws IOException {
        SummarizerOptions options = OptionsUtils.handleOptions(SummarizerOptions.class, args);
        summarizeEvents(options.eventFilePath(), options.summaryFilePath());
    }

    private Summarizer() {}

    @VisibleForTesting
    static void summarizeEvents(String eventFilePath, String summaryFilePath) throws IOException {
        Stream<BasicQueryEvent> eventStream = getEventStream(eventFilePath);
        Map<Instant, Long> summarizeEvents = createSummary(eventStream);
        writeEventSummary(summarizeEvents, summaryFilePath);
    }

    private static Stream<BasicQueryEvent> getEventStream(String path) throws IOException {
        ObjectReader objectReader = MapperUtils.getObjectMapper().readerFor(BasicQueryEvent.class);
        Stream<String> lines = path != null ? Files.lines(Paths.get(path)) :  getStdInLines();

        return lines
                .map(src -> {
                    try {
                        return objectReader.readValue(src);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private static Stream<String> getStdInLines() {
        Iterator<String> linesIterator = new Iterator<String>() {
            final Scanner linesScanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
            String currentVal;
            @Override
            public boolean hasNext() {
                if (linesScanner.hasNextLine()) {
                    String nextLine = linesScanner.nextLine();
                    if (!nextLine.isEmpty()) {
                        currentVal = nextLine;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String next() {
                if (currentVal == null && !hasNext()) throw new NoSuchElementException();
                return currentVal;
            }
        };

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(linesIterator, Spliterator.ORDERED),
                false);
    }

    private static Map<Instant, Long> createSummary(Stream<BasicQueryEvent> eventsStream) {
        return eventsStream
                .collect(Collectors.groupingBy(
                        queryEvent -> queryEvent.getMeta().getDt().atZone(ZoneOffset.UTC).withMinute(0)
                                .withSecond(0)
                                .withNano(0)
                                .toInstant(),
                        Collectors.counting()
                ));
    }

    @SuppressForbidden() //cli tool - System#out allowed
    private static void writeEventSummary(Map<Instant, Long> summary, String path) throws IOException {
        Stream<String> valueStream = summary.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> entry.getKey().toEpochMilli()))
                .map(entry -> entry.getKey().toString() + "," + entry.getValue());

        Stream<String> contentStream = Stream.concat(Stream.of("hour,count"), valueStream);
        if (path != null) {
            Files.write(Paths.get(path), (Iterable<String>) contentStream::iterator, StandardOpenOption.CREATE);
        } else {
            System.out.println(contentStream.collect(Collectors.joining("\n")));
        }
    }
}
