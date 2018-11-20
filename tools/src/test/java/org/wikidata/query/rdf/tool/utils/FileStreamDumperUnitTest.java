package org.wikidata.query.rdf.tool.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.wikidata.query.rdf.test.ManualClock;

import com.google.common.io.ByteStreams;

public class FileStreamDumperUnitTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void streamContentIsDumped() throws IOException {
        FileStreamDumper dumper = new FileStreamDumper(getDumpDir());

        try (InputStream in = dumper.wrap(testStream())) {
            ByteStreams.exhaust(in);
        }
        dumper.flush();

        assertThat(singleDumpFile()).hasContent("test stream\n");
    }

    @Test
    public void multipleStreamsAreConcatenated() throws IOException {
        FileStreamDumper dumper = new FileStreamDumper(getDumpDir());

        try (InputStream in = dumper.wrap(testStream())) {
            ByteStreams.exhaust(in);
        }

        try (InputStream in = dumper.wrap(testStream())) {
            ByteStreams.exhaust(in);
        }
        dumper.flush();

        assertThat(singleDumpFile()).hasContent("test stream\ntest stream");
    }

    @Test
    public void newFileIsCreatedOnRotation() throws IOException, InterruptedException {
        ManualClock clock = new ManualClock();
        FileStreamDumper dumper = new FileStreamDumper(getDumpDir(), clock, Duration.of(1, HOURS));

        try (InputStream in = dumper.wrap(testStream())) {
            ByteStreams.exhaust(in);
        }

        clock.sleep(Duration.of(2, MINUTES));
        Thread.sleep(1); // sleep to ensure timestamp is incremented
        dumper.rotate();
        try (InputStream in = dumper.wrap(testStream())) {
            ByteStreams.exhaust(in);
        }
        dumper.flush();

        assertThat(dumpFiles()).hasSize(2);
        dumpFiles().forEach(d -> assertThat(d).hasContent("test stream\n"));
    }

    @Test
    public void filesAreDeletedAfterDuration() throws IOException {
        ManualClock clock = new ManualClock();
        FileStreamDumper dumper = new FileStreamDumper(getDumpDir(), clock, Duration.of(1, HOURS));

        // creates 10 files over 10 minutes
        for (int i = 0; i < 10; i++) {
            try (InputStream in = dumper.wrap(testStream())) {
                ByteStreams.exhaust(in);
            }
            dumper.rotate();
            clock.sleep(Duration.of(1, MINUTES));
        }

        assertThat(dumpFiles()).hasSize(10);

        clock.sleep(Duration.of(61, MINUTES));

        // rotate 10 more times, since cleanup is done once every 20 rotations
        for (int i = 0; i < 10; i++) {
            dumper.rotate();
        }

        assertThat(dumpFiles()).isEmpty();
    }

    private Path singleDumpFile() throws IOException {
        assertThat(dumpFiles()).hasSize(1);
        return dumpFiles().findFirst().get();
    }

    private Stream<Path> dumpFiles() throws IOException {
        return Files.list(getDumpDir());
    }

    private Path getDumpDir() {
        return temporaryFolder.getRoot().toPath();
    }

    private ByteArrayInputStream testStream() {
        return new ByteArrayInputStream("test stream\n".getBytes(UTF_8));
    }

}
