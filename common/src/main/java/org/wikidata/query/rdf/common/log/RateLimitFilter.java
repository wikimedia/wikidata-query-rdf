package org.wikidata.query.rdf.common.log;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;

import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Logback {@link Filter} to limit logging events to a maximum rate.
 *
 * This is meant as a last resort to avoid overloading logging backends
 * (mainly logstash). Logging should still be tuned to avoid reaching this
 * limit at all. Once this filter starts dropping log events, it will not
 * differentiate and will silently drop all events that are over the limit.
 *
 * Monitoring of this filter should be done indirectly (logging rate in the
 * backend).
 *
 * This filter can be applied to any logback appender via the usual logback
 * config file.
 *
 * See <a href="https://logback.qos.ch/manual/filters.html">logback
 * documentation</a> for more details on filters.
 *
 * See <a href="https://github.com/vladimir-bukhtoyarov/bucket4j">bucket4j</a>
 * for more details on the token bucket implementation backing this filter.
 */
public class RateLimitFilter extends Filter<ILoggingEvent> {

    private TokenBucket bucket;
    private int bucketCapacity = 100;
    private Duration refillInterval = Duration.of(1, SECONDS);

    public RateLimitFilter() {
        this.bucket = createBucket(bucketCapacity, refillInterval);
    }

    public void setBucketCapacity(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
        bucket = createBucket(bucketCapacity, refillInterval);
    }

    public void setRefillIntervalInMillis(long refillIntervalInMillis) {
        this.refillInterval = Duration.ofMillis(refillIntervalInMillis);
        bucket = createBucket(bucketCapacity, refillInterval);
    }

    private static TokenBucket createBucket(int bucketCapacity, Duration refillInterval) {
        return TokenBuckets.builder()
                .withCapacity(bucketCapacity)
                .withFixedIntervalRefillStrategy(bucketCapacity, refillInterval.toMillis(), MILLISECONDS)
                .build();
    }

    /**
     * Deny logging if over limit, neutral otherwise.
     */
    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (bucket.tryConsume()) return NEUTRAL;
        return DENY;
    }

}
