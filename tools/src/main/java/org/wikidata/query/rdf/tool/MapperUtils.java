package org.wikidata.query.rdf.tool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Object mapping service class.
 */
public final class MapperUtils {
    // Initializing statically as instructed in https://github.com/FasterXML/jackson-modules-java8/tree/master/datetime
    private static final ObjectMapper MAPPER;
    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.registerModule(new JavaTimeModule());
    }

    @SuppressFBWarnings("MS_EXPOSE_REP")
    public static ObjectMapper getObjectMapper() {
        return MAPPER;
    }

    private MapperUtils() {}
}
