package org.wikidata.query.rdf.updater.consumer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class KafkaStreamConsumerMetricsListener {
    private final Counter triplesAccum;
    private final Counter invalidDuplicates;
    private final Counter triplesOffered;
    private final Counter deletedEntities;
    private final AtomicLong lag = new AtomicLong();
    private final Clock clock;

    KafkaStreamConsumerMetricsListener(MetricRegistry registry, Clock clock) {
        triplesAccum = registry.counter("kafka-stream-consumer-triples-accumulated");
        invalidDuplicates = registry.counter("kafka-stream-consumer-triples-invalid-duplicates");
        triplesOffered = registry.counter("kafka-stream-consumer-triples-offered");
        deletedEntities = registry.counter("kafka-stream-consumer-deleted-entities");
        registry.gauge("kafka-stream-consumer-lag", () -> lag::get);
        this.clock = clock;
    }

    public static KafkaStreamConsumerMetricsListener forRegistry(MetricRegistry registry) {
        return new KafkaStreamConsumerMetricsListener(registry, Clock.systemUTC());
    }

    void triplesAccum(int triples) {
        triplesAccum.inc(triples);
    }

    void invalidDuplicates(int triples) {
        invalidDuplicates.inc(triples);
    }

    void triplesOffered(int triples) {
        triplesOffered.inc(triples);
    }

    void deletedEntities(int deletes) {
        deletedEntities.inc(deletes);
    }

    void lag(Instant lastEventTime) {
        lag.set(Duration.between(lastEventTime, clock.instant()).toMillis());
    }
}
