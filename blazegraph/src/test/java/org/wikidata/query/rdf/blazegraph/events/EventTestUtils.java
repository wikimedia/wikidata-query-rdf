package org.wikidata.query.rdf.blazegraph.events;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class EventTestUtils {
    private EventTestUtils() {}
    public static TestEvent newTestEvent() {
        EventMetadata meta = new EventMetadata("requestId", "id", Instant.EPOCH, "domain", "stream");
        return new TestEvent("data", meta);
    }

    public static String testEventJsonString() {
        return "{" +
                "\"data\":\"data\"," +
                "\"meta\":{" +
                "\"id\":\"id\"," +
                "\"dt\":\"1970-01-01T00:00:00Z\"," +
                "\"request_id\":\"requestId\"," +
                "\"domain\":\"domain\"," +
                "\"stream\":\"stream\"" +
                "}}";
    }

    public static class TestEvent implements Event {
        private final String data;
        private final EventMetadata metadata;

        public TestEvent(String data, EventMetadata metadata) {
            this.data = data;
            this.metadata = metadata;
        }

        @Override
        public EventMetadata getMetadata() {
            return metadata;
        }

        @JsonProperty("data")
        public String getData() {
            return data;
        }
    }
}
