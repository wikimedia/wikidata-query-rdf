package org.wikidata.query.rdf.common.log;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;

public class PerLoggerThrottlerUnitTest {

    public static final int THRESHOLD = 2;
    public static final Duration DURATION = Duration.ofMinutes(1);
    public static final int CACHE_SIZE = 10;
    private AtomicReference<Instant> time = new AtomicReference<>(Instant.now());

    @Test
    public void eventsBelowThresholdAreNeutral() {
        PerLoggerThrottler throttler = createThrottler();

        for (int i = 0; i < THRESHOLD; i++) {
            assertThat(throttler.decide(logEvent())).isEqualTo(NEUTRAL);
        }
    }

    @Test
    public void eventsOverThresholdAreDenied() {
        PerLoggerThrottler throttler = createThrottler();

        for (int i = 0; i < THRESHOLD; i++) {
            throttler.decide(logEvent());
        }

        assertThat(throttler.decide(logEvent())).isEqualTo(DENY);
    }

    @Test
    public void eventsAreNeutralAgainAfterPeriod() {
        PerLoggerThrottler throttler = createThrottler();

        for (int i = 0; i < THRESHOLD; i++) {
            throttler.decide(logEvent());
        }
        assertThat(throttler.decide(logEvent())).isEqualTo(DENY);

        waitFor(DURATION);

        assertThat(throttler.decide(logEvent())).isEqualTo(NEUTRAL);
    }

    @Test
    public void droppedMessagesAreReportedOnExpiration() {
        Logger rootLogger = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
        Appender appender = mock(Appender.class);
        ArgumentCaptor<LoggingEvent> eventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        rootLogger.addAppender(appender);

        PerLoggerThrottler throttler = createThrottler();

        // consume all event which can pass the filter
        for (int i = 0; i < THRESHOLD; i++) {
            throttler.decide(logEvent());
        }

        // all those events should be DENIED
        for (int i = 0; i < 50; i++) {
            throttler.decide(logEvent());
        }

        waitFor(DURATION);

        // this event should pass again
        assertThat(throttler.decide(logEvent())).isEqualTo(NEUTRAL);

        verify(appender).doAppend(eventCaptor.capture());

        LoggingEvent event = eventCaptor.getValue();
        assertThat(event.getLoggerName()).isEqualTo("my.logger");
        assertThat(event.getFormattedMessage()).isEqualTo("Message [my message] was duplicated 50 times.");
    }

    private PerLoggerThrottler createThrottler() {
        PerLoggerThrottler throttler = new PerLoggerThrottler();
        throttler.setCacheSize(CACHE_SIZE);
        throttler.setPeriodInMillis(DURATION.toMillis());
        throttler.setThreshold(THRESHOLD);
        throttler.setTicker(new Ticker() {
            @Override
            public long read() {
                return NANOSECONDS.convert(time.get().toEpochMilli(), MILLISECONDS);
            }
        });
        return throttler;
    }

    private LoggingEvent logEvent() {
        LoggingEvent event = new LoggingEvent();
        event.setLoggerName("my.logger");
        event.setMessage("my message");
        return event;
    }

    private void waitFor(Duration duration) {
        Instant instant = time.get();
        time.set(instant.plus(duration));
    }
}
