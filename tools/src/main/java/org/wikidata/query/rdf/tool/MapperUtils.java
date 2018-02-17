package org.wikidata.query.rdf.tool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Object mapping service class.
 */
public final class MapperUtils {
    // Initializing statically as instructed in https://github.com/FasterXML/jackson-modules-java8/tree/master/datetime
    private static final ObjectMapper mapper;
    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new JavaTimeModule());
    }

    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

    private MapperUtils() {}
}
