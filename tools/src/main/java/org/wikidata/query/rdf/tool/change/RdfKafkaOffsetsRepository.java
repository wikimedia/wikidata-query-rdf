package org.wikidata.query.rdf.tool.change;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.wikidata.query.rdf.tool.Utils.loadBody;

import java.net.URI;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.wikidata.query.rdf.tool.rdf.UpdateBuilder;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;

import com.google.common.collect.Multimap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Store and retrieve Kafka offsets from a permanent storage.
 *
 * This implementation uses Blazegraph as storage.
 */
public class RdfKafkaOffsetsRepository implements KafkaOffsetsRepository {
    /** Offsets fetch query. */
    private static final String GET_OFFSETS = loadBody("GetKafkaOffsets", RdfKafkaOffsetsRepository.class);
    /** Offsets update query. */
    private static final String UPDATE_OFFSETS = loadBody("updateOffsets", RdfKafkaOffsetsRepository.class);

    /**
     * Root of the URI hierarchy.
     *
     * Used for storing Kafka metadata in RDF store.
     */
    private final URI root;

    /** Used to connect to the RDF store. */
    private final RdfClient rdfClient;

    public RdfKafkaOffsetsRepository(URI root, RdfClient rdfClient) {
        this.root = root;
        this.rdfClient = rdfClient;
    }

    /**
     * Load Kafka offsets from storage.
     *
     * @return Mutable map that contains offsets per TopicPartition. Can be empty.
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> load(Instant firstStartTime) {
        UpdateBuilder ub = new UpdateBuilder(GET_OFFSETS);
        ub.bindUri("root", root);
        Multimap<String, String> result = rdfClient.selectToMap(ub.toString(), "topic", "offset");

        // Create a map of offsets from storage
        return result.entries()
                .stream().collect(toImmutableMap(
                        e -> {
                            String[] parts = e.getKey().split(":", 2);
                            return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
                        },
                        e -> new OffsetAndTimestamp(Integer.parseInt(e.getValue()), firstStartTime.toEpochMilli())
                ));
    }

    /**
     * Store offsets in a permanent storage.
     */
    @Override
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "we want to be platform independent here.")
    public void store(Map<TopicPartition, Long> partitionsAndOffsets) {
        UpdateBuilder ub = new UpdateBuilder(UPDATE_OFFSETS);
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<TopicPartition, Long> partitionAndOffset : partitionsAndOffsets.entrySet()) {
            TopicPartition tp = partitionAndOffset.getKey();
            Long offset = partitionAndOffset.getValue();
            sb.append(String.format(Locale.ROOT,
                    "<%s> wikibase:kafka ( \"%s:%d\" %d ) .\n",
                    root, tp.topic(), tp.partition(), offset));
        }

        ub.bindUri("root", root);
        ub.bind("data", sb.toString());
        rdfClient.update(ub.toString());
    }
}
