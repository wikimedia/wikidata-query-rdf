package org.wikidata.query.rdf.test;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

public class ManualClockUnitTest {

    @Test
    public void sleepForTheProperAmountOfTime() {
        Instant initialTime = Instant.ofEpochMilli(1000);
        ManualClock clock = new ManualClock(initialTime);

        clock.sleep(Duration.of(10, SECONDS));

        assertThat(clock.instant(), equalTo(initialTime.plusSeconds(10)));
    }

    @Test
    public void sleepUntilTheProperInstant() {
        Instant initialTime = Instant.ofEpochMilli(1000);
        Instant endTime = Instant.ofEpochMilli(100000);
        ManualClock clock = new ManualClock(initialTime);

        clock.sleepUntil(endTime);

        assertThat(clock.instant(), equalTo(endTime));
    }

    @Test(expected = IllegalArgumentException.class)
    public void cantGoBackInTime() {
        Instant initialTime = Instant.ofEpochMilli(10000);
        Instant endTime = Instant.ofEpochMilli(100);
        ManualClock clock = new ManualClock(initialTime);

        clock.sleepUntil(endTime);
    }

}
