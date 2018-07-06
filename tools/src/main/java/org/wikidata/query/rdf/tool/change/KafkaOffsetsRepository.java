package org.wikidata.query.rdf.tool.change;

import static java.util.stream.Collectors.toMap;
import static org.wikidata.query.rdf.tool.Utils.loadBody;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nullable;

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
 * The offsets are stored as:
 * {@code
 * <root> wikibase:kafka ("topic:partition" offset)
 * }
 * e.g.:
 * {@code
 * <http://www.wikidata.org>  wikibase:kafka ("mediawiki-create:0" 123445)
 * }
 * root is the base concept URI for wikibase data, e.g.
 * http://www.wikidata.org for wikidata.
 */
public class KafkaOffsetsRepository {
    /** Offsets fetch query. */
    private static final String GET_OFFSETS = loadBody("GetKafkaOffsets", KafkaOffsetsRepository.class);
    /** Offsets update query. */
    private static final String UPDATE_OFFSETS = loadBody("updateOffsets", KafkaOffsetsRepository.class);

    /**
     * Root of the URI hierarchy.
     *
     * Used for storing Kafka metadata in RDF store.
     */
    private final URI root;

    /**
     * Null if we won't be storing Kafka offsets in RDF repository.
     */
    @Nullable
    private final RdfClient rdfClient;

    public KafkaOffsetsRepository(URI root, RdfClient rdfClient) {
        this.root = root;
        this.rdfClient = rdfClient;
    }

    /**
     * Load Kafka offsets from storage.
     *
     * @return Mutable map that contains offsets per TopicPartition. Can be empty.
     */
    public Map<TopicPartition, OffsetAndTimestamp> load(Instant firstStartTime) {
        if (rdfClient == null) {
            // For tests, we may ignore RDF repo and pass null here.
            return new HashMap<>();
        }
        UpdateBuilder ub = new UpdateBuilder(GET_OFFSETS);
        ub.bindUri("root", root);
        Multimap<String, String> result = rdfClient.selectToMap(ub.toString(), "topic", "offset");

        // Create a map of offsets from storage
        return result.entries()
                .stream().collect(toMap(
                        e -> {
                            String[] parts = e.getKey().split(":", 2);
                            return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
                        },
                        e -> new OffsetAndTimestamp(Integer.parseInt(e.getValue()), firstStartTime.toEpochMilli())
                ));
    }

    /** Store offsets in a permanent storage. */
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "we want to be platform independent here.")
    public void store(Map<TopicPartition, Long> partitionsAndOffsets) {
        if (rdfClient == null) {
            return;
        }

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
