package org.wikidata.query.rdf.updater.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.Test;
import org.wikidata.query.rdf.tool.MapperUtils;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MutationEventDataJsonKafkaDeserializerUnitTest {
    private final MutationEventDataJsonKafkaDeserializer kafkaDeserializer = new MutationEventDataJsonKafkaDeserializer();
    private final ObjectMapper mapper = MapperUtils.getObjectMapper();

    private MutationEventData buildTestEvent(MutationEventDataFactory.MutationBuilder builder) {
        return builder.buildMutation(new EventsMeta(Instant.EPOCH, "id", "domain", "stream", "requestId"),
                "Q1", 123L, Instant.now(), 0, 1, "delete");

    }

    @Test
    public void testDeserializationV1() throws JsonProcessingException {
        MutationEventData event = buildTestEvent(MutationEventDataFactory.v1().getMutationBuilder());
        MutationEventData actual = kafkaDeserializer.deserialize("unused", mapper.writeValueAsBytes(event));
        assertThat(event).isEqualTo(actual);
    }

    @Test
    public void testDeserializationV2() throws JsonProcessingException {
        MutationEventData event = buildTestEvent(MutationEventDataFactory.v2().getMutationBuilder());
        MutationEventData actual = kafkaDeserializer.deserialize("unused", mapper.writeValueAsBytes(event));
        assertThat(event).isEqualTo(actual);
    }

    @Test
    public void testBrokenJson() {
        MutationEventData actual = kafkaDeserializer.deserialize("unused", new byte[]{'[', '}'});
        assertThat(actual).isNull();
    }
}
