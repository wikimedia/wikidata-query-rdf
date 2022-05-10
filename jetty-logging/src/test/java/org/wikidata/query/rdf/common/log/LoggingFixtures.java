package org.wikidata.query.rdf.common.log;

import ch.qos.logback.classic.spi.LoggingEvent;

/**
 * Some fixtures to help test logging related code.
 */
public final class LoggingFixtures {

    public static final String LOGGER_NAME = "my.logger";
    public static final String MESSAGE = "my message";

    private LoggingFixtures() {
        // Utility class should not be constructed
    }

    /**
     * A {@link LoggingEvent} with a known logger name and message.
     */
    public static LoggingEvent logEvent() {
        LoggingEvent event = new LoggingEvent();
        event.setLoggerName(LOGGER_NAME);
        event.setMessage(MESSAGE);
        return event;
    }
}
