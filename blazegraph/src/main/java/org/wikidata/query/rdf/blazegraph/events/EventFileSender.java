package org.wikidata.query.rdf.blazegraph.events;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;


@NotThreadSafe //EventFileSender is wrapped around in BufferedEventSender that serializes event sending
public class EventFileSender implements EventSender {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ObjectWriter objectWriter;
    private final Writer writer;

    public EventFileSender(Path path) {
        objectWriter = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(AUTO_CLOSE_TARGET)
                .writer();
        try {
            OutputStream outputStream = Files.newOutputStream(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean push(Event event) {
        return push(Collections.singleton(event)) == 1;
    }

    @Override
    public int push(Collection<Event> events) {
        AtomicInteger written = new AtomicInteger();
        try {
            events.forEach(value -> {
                try {
                    objectWriter.writeValue(writer, value);
                    writer.write("\n");
                    written.incrementAndGet();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            writer.flush();
        } catch (IOException e) {
            log.error("Couldn't write events", e);
            return written.intValue();
        }
        return events.size();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
