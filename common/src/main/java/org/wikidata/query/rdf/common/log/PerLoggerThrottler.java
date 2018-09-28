package org.wikidata.query.rdf.common.log;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static com.google.common.base.Ticker.systemTicker;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Throttle log messages per logger.
 *
 * In some cases, a single logger will start generating huge amount of
 * messages. Throttling per logger can ensure that the logging backend (for
 * example logstash) is not overloaded, while keeping low frequency messages
 * flowing.
 *
 * This filter can be applied to any logback appender via the usual logback
 * config file.
 *
 * See <a href="https://logback.qos.ch/manual/filters.html">logback
 * documentation</a> for more details on filters.
 *
 * See <a href="https://github.com/vladimir-bukhtoyarov/bucket4j">bucket4j</a>
 * for more details on the token bucket implementation backing this filter.
 *
 * Note that instance variables of this class are not synchronized or final.
 * They are set only at filter initialization (via Joran) and are assumed to be
 * safely published.
 */
public class PerLoggerThrottler extends Filter<ILoggingEvent> {

    private static final Logger log = LoggerFactory.getLogger(PerLoggerThrottler.class);

    /**
     * Maps each logger/message to a counter.
     *
     * We use an AtomicLong and not a LongAdder since we need to retrieve the
     * count each time it is incremented.
     */
    private LoadingCache<Key, AtomicLong> messagesCount;

    /**
     * Threshold over which logs are discarded.
     */
    private long threshold = 100;

    /**
     * Number of log entries to keep track of.
     */
    private long cacheSize = 1000;

    /**
     * Period over which to count messages.
     */
    private Duration period = Duration.of(1, MINUTES);

    /**
     * Ticker used by the cache holding the messagesCount.
     *
     * Only useful for testing.
     */
    private Ticker ticker = systemTicker();

    public PerLoggerThrottler() {
        this.messagesCount = createMessagesCount(period, cacheSize, ticker);
    }

    private LoadingCache<Key, AtomicLong> createMessagesCount(Duration period, long cacheSize, Ticker ticker) {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(period.toMillis(), MILLISECONDS)
                .maximumSize(cacheSize)
                .removalListener(this::onRemoval)
                .ticker(ticker)
                .build(new CacheLoader<Key, AtomicLong>() {
                    @Override
                    public AtomicLong load(Key key) {
                        return new AtomicLong();
                    }
                });
    }

    /**
     * Called each time an entry is removed from {@code messagesCount}.
     */
    private void onRemoval(Map.Entry<Key, AtomicLong> notification) {
        long nbSuppressedMessages = notification.getValue().get() - threshold;

        if (nbSuppressedMessages > 0) {
            LoggerFactory.getLogger(notification.getKey().loggerName)
                    .warn(
                            "Message [{}] was duplicated {} times.",
                            notification.getKey().messageTemplate,
                            nbSuppressedMessages);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FilterReply decide(ILoggingEvent event) {
        try {
            AtomicLong count = messagesCount.get(new Key(event.getLoggerName(), event.getMessage()));
            long newCount = count.incrementAndGet();
            if (newCount > threshold) return DENY;
            return NEUTRAL;
        } catch (ExecutionException ignore) {
            // It should always be possible to create a LongAdder, in the
            // unlikely case where it isn't, let's fail closed.
            return NEUTRAL;
        }
    }

    /**
     * Number of log entries to keep track of.
     *
     * Used by Joran to configure this filter.
     */
    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
        this.messagesCount = createMessagesCount(period, cacheSize, ticker);
    }

    /**
     * Period over which to count messages.
     *
     * Used by Joran to configure this filter.
     */
    public void setPeriodInMillis(long periodInMillis) {
        this.period = Duration.of(periodInMillis, MILLIS);
        this.messagesCount = createMessagesCount(period, cacheSize, ticker);
    }

    /**
     * Threshold over which logs are discarded.
     *
     * Used by Joran to configure this filter.
     */
    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    /**
     * Ticker used by the cache holding the messagesCount.
     *
     * Only useful for testing.
     */
    @VisibleForTesting
    void setTicker(Ticker ticker) {
        this.ticker = ticker;
        this.messagesCount = createMessagesCount(period, cacheSize, ticker);
    }

    private static final class Key {
        private final String loggerName;
        private final String messageTemplate;

        private Key(String loggerName, String messageTemplate) {
            this.loggerName = loggerName;
            this.messageTemplate = messageTemplate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(loggerName, key.loggerName) &&
                    Objects.equals(messageTemplate, key.messageTemplate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(loggerName, messageTemplate);
        }
    }
}
