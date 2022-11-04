package org.wikidata.query.rdf.tool.change;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.wikidata.query.rdf.tool.MapperUtils.getObjectMapper;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserializer for Kafka stream objects from JSON.
 * This class allows to deserialize a family of related classes.
 *
 * @param <T> Type to be deserialized into.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDeserializer.class);

    private final ObjectMapper mapper = getObjectMapper();

    private final Map<String, Class<? extends T>> topicToClass;

    public JsonDeserializer(Map<String, Class<? extends T>> topicToClass) {
        this.topicToClass = topicToClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // all configuration is through constructor
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;

        try {
            return mapper.readValue(data, topicToClass.get(topic));
        } catch (IOException e) {
            String dataString = new String(data, UTF_8);
            LOGGER.warn("Data in topic {} cannot be deserialized [{}].", topic, dataString, e);
            return null;
        }
    }

    @Override
    public void close() {
        // NOOP
    }
}
