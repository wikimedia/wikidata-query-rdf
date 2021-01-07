package org.wikidata.query.rdf.tool.change;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClient;
import static org.wikidata.query.rdf.tool.HttpClientUtils.buildHttpClientRetryer;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyHost;
import static org.wikidata.query.rdf.tool.HttpClientUtils.getHttpProxyPort;
import static org.wikidata.query.rdf.tool.RdfRepositoryForTesting.url;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.jetty.client.HttpClient;
import org.junit.Test;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

public class RdfKafkaRepositoryIntegrationTest {
    private static final long BEGIN_DATE = 1518207153000L;

    @Test
    public void readWriteOffsets() throws Exception {
        Uris uris = Uris.fromString("https://acme.test", singleton(0L));

        Instant startTime = Instant.ofEpochMilli(BEGIN_DATE);

        HttpClient httpClient = buildHttpClient(getHttpProxyHost(), getHttpProxyPort());
        RdfClient rdfClient = new RdfClient(httpClient,
                url("/namespace/wdq/sparql"),
                buildHttpClientRetryer(),
                Duration.of(-1, SECONDS)
        );

        try {
            rdfClient.update("CLEAR ALL");
            KafkaOffsetsRepository kafkaOffsetsRepository = new RdfKafkaOffsetsRepository(uris.builder().build(), rdfClient);

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition("topictest", 0), new OffsetAndMetadata(1L));
            offsets.put(new TopicPartition("othertopic", 0), new OffsetAndMetadata(2L));
            kafkaOffsetsRepository.store(offsets);

            Map<TopicPartition, OffsetAndTimestamp> offsetsAndTimestamps = kafkaOffsetsRepository.load(startTime);
            assertThat(offsetsAndTimestamps.get(new TopicPartition("topictest", 0)).offset()) .isEqualTo(1L);
            assertThat(offsetsAndTimestamps.get(new TopicPartition("othertopic", 0)).offset()).isEqualTo(2L);

            offsets = new HashMap<>();
            offsets.put(new TopicPartition("topictest", 0), new OffsetAndMetadata(3L));
            offsets.put(new TopicPartition("othertopic", 0), new OffsetAndMetadata(4L));
            kafkaOffsetsRepository.store(offsets);
            offsetsAndTimestamps = kafkaOffsetsRepository.load(startTime);
            assertThat(offsetsAndTimestamps.get(new TopicPartition("topictest", 0)).offset()) .isEqualTo(3L);
            assertThat(offsetsAndTimestamps.get(new TopicPartition("othertopic", 0)).offset()).isEqualTo(4L);
        } finally {
            rdfClient.update("CLEAR ALL");
            httpClient.stop();
        }
    }

}
