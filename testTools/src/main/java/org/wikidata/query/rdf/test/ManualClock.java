package org.wikidata.query.rdf.test;

import static java.time.ZoneOffset.UTC;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ManualClock extends Clock {

    @Nonnull
    private final ZoneId zone;

    @Nonnull
    private Instant now;

    public ManualClock() {
        this(UTC, Instant.now());
    }

    public ManualClock(Instant now) {
        this(UTC, now);
    }

    public ManualClock(ZoneId zone, Instant now) {
        this.zone = zone;
        this.now = now;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public synchronized Clock withZone(ZoneId zone) {
        return new ManualClock(zone, now);
    }

    @Override
    public synchronized Instant instant() {
        return now;
    }

    public synchronized void sleep(TemporalAmount duration) {
        now = now.plus(duration);
    }

    public synchronized void sleepUntil(Instant until) {
        if (until.isBefore(now)) throw new IllegalArgumentException("Can't go back in time");
        now = until;
    }
}
