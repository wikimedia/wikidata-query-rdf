package org.wikidata.query.rdf.updater.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataJsonDeserializer;

public class MutationEventDataJsonKafkaDeserializer implements Deserializer<MutationEventData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MutationEventDataJsonKafkaDeserializer.class);
    private final MutationEventDataJsonDeserializer deserializer = new MutationEventDataJsonDeserializer();

    @Override
    public MutationEventData deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return deserializer.deserialize(bytes);
        } catch (IOException e) {
            String dataString = new String(bytes, UTF_8);
            LOGGER.warn("Data in topic {} cannot be deserialized [{}].", topic, dataString, e);
            return null;
        }
    }
}
