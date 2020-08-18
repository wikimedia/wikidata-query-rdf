package org.wikidata.query.rdf.tool.eventsummmary;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SummarizerUnitTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testMultiHourStream() throws Exception {
        String eventFile = getClass().getClassLoader().getResource("events.log").toURI().getPath();
        File summaryFile = temporaryFolder.newFile();

        int noOfHours = 10;

        Summarizer.summarizeEvents(eventFile, summaryFile.getPath());

        Scanner scanner = new Scanner(summaryFile, "UTF-8");
        scanner.useDelimiter(",|\\n");

        assertThat(scanner.next()).isEqualTo("hour");
        assertThat(scanner.next()).isEqualTo("count");

        for (int i = 0; i < noOfHours; i++) {
            String date = scanner.next();
            assertThat(Instant.parse(date)).isEqualTo(getStartHourInstant(i));
            assertThat(scanner.nextLong()).isEqualTo(1 + i);
        }
    }

    private Instant getStartHourInstant(int i) {
        return Instant.ofEpochMilli(Duration.ofHours(i).toMillis());
    }
}
