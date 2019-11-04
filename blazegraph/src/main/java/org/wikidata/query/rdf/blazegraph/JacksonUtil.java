package org.wikidata.query.rdf.blazegraph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JacksonUtil {
    public static final ObjectWriter DEFAULT_OBJECT_WRITER;
    public static final ObjectReader DEFAULT_OBJECT_READER;

    static {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        DEFAULT_OBJECT_READER = mapper.reader();
        DEFAULT_OBJECT_WRITER = mapper.writer();
    }

    private JacksonUtil() {}
}
