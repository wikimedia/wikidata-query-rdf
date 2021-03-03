package org.wikidata.query.rdf.tool.change;

import static java.time.Instant.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;

@RunWith(MockitoJUnitRunner.class)
public class RdfKafkaOffsetsRepositoryUnitTest {

    @Mock private RdfClient rdfClient;
    @Captor private ArgumentCaptor<String> queryCaptor;

    private RdfKafkaOffsetsRepository repository;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.of(
            new TopicPartition("topic-1", 2), new OffsetAndMetadata(3L),
            new TopicPartition("topic-4", 5), new OffsetAndMetadata(6L),
            new TopicPartition("topic-7", 8), new OffsetAndMetadata(2149809252L)
    );

    @Before
    public void setupRepositoryUnderTest() throws URISyntaxException {
        URI root = Uris.fromString("https://acme.test").builder().build();
        repository = new RdfKafkaOffsetsRepository(root, rdfClient);
    }

    @Test
    public void correctQueryIsConstructedOnLoad() {
        when(rdfClient.selectToMap(queryCaptor.capture(), eq("topic"), eq("offset")))
                .thenReturn(ImmutableSetMultimap.of());

        repository.load(now());

        assertThat(queryCaptor.getValue())
                .containsSubsequence(
                        "SELECT ?topic ?offset WHERE {",
                        "  <https://acme.test> wikibase:kafka (?topic ?offset)",
                        "}");
    }

    @Test
    public void offsetsAreParsedOnLoad() {
        Instant startTime = now();
        long startTimestamp = startTime.toEpochMilli();

        when(rdfClient.selectToMap(any(), any(), any()))
                .thenReturn(ImmutableSetMultimap.of(
                        "topic-1:2", "3",
                        "topic-4:5", "6",
                        "topic-7:8", "2149809252"
                ));

        Map<TopicPartition, OffsetAndTimestamp> loadedOffsets = repository.load(startTime);

        assertThat(loadedOffsets)
                .containsEntry(
                        new TopicPartition("topic-1", 2),
                        new OffsetAndTimestamp(3, startTimestamp)
                )
                .containsEntry(
                        new TopicPartition("topic-4", 5),
                        new OffsetAndTimestamp(6, startTimestamp)
                )
                .containsEntry(
                        new TopicPartition("topic-7", 8),
                        new OffsetAndTimestamp(2149809252L, startTimestamp)
                );
    }

    @Test
    public void offsetsAreStored() {
        repository.store(offsets);

        verify(rdfClient).update(queryCaptor.capture());

        assertThat(queryCaptor.getValue())
                .containsSubsequence(
                        "DELETE {",
                        "  ?z rdf:first ?head ; rdf:rest ?tail .",
                        "}",
                        "WHERE {",
                        "  [] wikibase:kafka ?list .",
                        "  ?list rdf:rest* ?z .",
                        "  ?z rdf:first ?head ; rdf:rest ?tail .",
                        "};")
                .containsSubsequence(
                        "DELETE WHERE {",
                        "  <https://acme.test> wikibase:kafka ?o .",
                        "};")
                .containsSubsequence(
                        "INSERT DATA {",
                        "  <https://acme.test> wikibase:kafka ( \"topic-1:2\" 3 ) .",
                        "<https://acme.test> wikibase:kafka ( \"topic-4:5\" 6 ) .",
                        "<https://acme.test> wikibase:kafka ( \"topic-7:8\" 2149809252 ) .",
                        "}"
                );
    }

}
