package org.wikidata.query.rdf.tool.utils;

import static java.nio.file.Files.delete;
import static java.nio.file.Files.getLastModifiedTime;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.HOURS;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wraps {@link OutputStream}s and send a copy to file once it has been read.
 *
 * The output file will be rotated on demand. Files older than 1h will be deleted.
 */
public class FileStreamDumper implements StreamDumper {

    private static final Logger LOG = LoggerFactory.getLogger(FileStreamDumper.class);

    private static final String DUMP_FILE_SUFFIX = ".dump";

    private final Path dumpDir;
    public final Duration keepDumpsDuration;
    private final Clock clock;

    private OutputStream dumpOutput;

    private final AtomicLong numberOfRotations = new AtomicLong();

    public FileStreamDumper(Path dumpDir) {
        this(dumpDir, Clock.systemUTC(), Duration.of(1, HOURS));
    }

    /** Creates a StreamDumper that will dump to {@code dumpDir}. */
    public FileStreamDumper(Path dumpDir, Clock clock, Duration keepDumpsDuration) {
        this.dumpDir = dumpDir;
        this.clock = clock;
        dumpOutput = createDumpFile();
        this.keepDumpsDuration = keepDumpsDuration;
    }

    @Override
    public InputStream wrap(InputStream inputStream) {
        if (inputStream == null) return null;

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        OutputStream tee = new FilterOutputStream(buffer) {
            @Override
            public void close() throws IOException {
                synchronized (FileStreamDumper.this) {
                    dumpOutput.write(buffer.toByteArray());
                }
                super.close();
            }
        };

        return new TeeInputStream(inputStream, tee, true);
    }

    @Override
    public void rotate() {
        synchronized (this) {
            closeCurrentOutput();
            dumpOutput = createDumpFile();
        }

        // cleanup once per 20 runs
        if (numberOfRotations.incrementAndGet() % 20 == 0) {
            cleanDumps();
        }
    }

    @VisibleForTesting
    synchronized void flush() throws IOException {
        dumpOutput.flush();
    }

    private void closeCurrentOutput() {
        try {
            dumpOutput.close();
        } catch (IOException ioe) {
            LOG.info("Could not close dump file, some data might be lost", ioe);
        }
    }

    private OutputStream createDumpFile() {
        try {
            long timestamp = clock.millis();
            final Path dumpFile = dumpDir.resolve(String.valueOf(timestamp) + DUMP_FILE_SUFFIX);
            return new BufferedOutputStream(Files.newOutputStream(dumpFile, StandardOpenOption.CREATE));
        } catch (IOException ioe) {
            LOG.info("Could not create dump file, dump will be ignored.");
            return new NullOutputStream();
        }
    }

    /**
     * Periodically clean up old dumps.
     */
    private void cleanDumps() {
        Instant cutoff = now(clock).minus(keepDumpsDuration);

        try (Stream<Path> dumpFiles = Files.list(dumpDir)) {
            dumpFiles
                    .filter(p -> {
                        try {
                            return p.toString().endsWith(DUMP_FILE_SUFFIX)
                                    && getLastModifiedTime(p).toInstant().isBefore(cutoff);
                        } catch (IOException e) {
                            return false;
                        }
                    })
                    .forEach(p -> {
                        try {
                            delete(p);
                        } catch (IOException e) {
                            LOG.info("Could not delete {}.", p);
                        }
                    });
        } catch (IOException e) {
            LOG.info("Could not list files in {}", dumpDir, e);
        }
    }

}
