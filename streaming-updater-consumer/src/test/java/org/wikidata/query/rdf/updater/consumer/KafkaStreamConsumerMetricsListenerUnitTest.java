package org.wikidata.query.rdf.updater.consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers;
import org.wikidata.query.rdf.updater.DiffEventData;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.RDFChunkDeserializer;
import org.wikidata.query.rdf.updater.RDFDataChunk;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamConsumerMetricsListenerUnitTest {
    @Mock
    private KafkaConsumer<String, MutationEventData> consumer;
    private final RDFChunkDeserializer chunkDeser = new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance()));

    @Test
    public void test_metrics_are_reported() {
        Instant now = Instant.now();
        Clock fixedClock = Clock.fixed(now, ZoneOffset.UTC);
        Duration lagEvt1 = Duration.ofHours(2);
        Duration lagEvt2 = Duration.ofHours(1);
        Instant evTime1 = now.minus(lagEvt1);
        Instant evTime2 = now.minus(lagEvt2);
        MutationEventData msg1 = new DiffEventData(new EventsMeta(Instant.now(), "unused", "domain", "stream", "req"),
                "Q0", 1, evTime1, 0, 1, MutationEventData.IMPORT_OPERATION,
                new RDFDataChunk("\n<uri:a> <uri:a> <uri:a> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                null, null, null);
        MutationEventData msg2 = new DiffEventData(new EventsMeta(Instant.now(), "unused", "domain", "stream", "req"),
                "Q0", 2, evTime2, 0, 1, MutationEventData.IMPORT_OPERATION,
                new RDFDataChunk("\n<uri:b> <uri:b> <uri:b> .\n", RDFFormat.TURTLE.getDefaultMIMEType()),
                null, null, null);

        TopicPartition topicPartition = new TopicPartition("topic", 0);
        when(consumer.poll(anyLong())).thenReturn(
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 0, null, msg1)))),
                new ConsumerRecords<>(singletonMap(topicPartition,
                        singletonList(new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(), 1, null, msg2))))
        );

        MetricRegistry registry = new MetricRegistry();
        KafkaStreamConsumer streamConsumer = new KafkaStreamConsumer(consumer, topicPartition, chunkDeser, 1,
                new KafkaStreamConsumerMetricsListener(registry, fixedClock));
        streamConsumer.poll(9);
        Gauge<Long> lag = registry.getGauges().get("kafka-stream-consumer-lag");
        Counter offered = registry.getCounters().get("kafka-stream-consumer-triples-offered");
        Counter accumulated = registry.getCounters().get("kafka-stream-consumer-triples-accumulated");

        assertThat(lag.getValue()).isZero();
        assertThat(offered.getCount()).isEqualTo(1);
        assertThat(accumulated.getCount()).isEqualTo(1);
        streamConsumer.acknowledge();
        assertThat(lag.getValue()).isEqualTo(lagEvt1.toMillis());

        streamConsumer.poll(9);
        assertThat(offered.getCount()).isEqualTo(2);
        assertThat(accumulated.getCount()).isEqualTo(2);
        assertThat(lag.getValue()).isEqualTo(lagEvt1.toMillis());

        streamConsumer.acknowledge();
        assertThat(lag.getValue()).isEqualTo(lagEvt2.toMillis());
    }
}
