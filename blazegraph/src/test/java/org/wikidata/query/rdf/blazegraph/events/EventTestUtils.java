package org.wikidata.query.rdf.blazegraph.events;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class EventTestUtils {
    private EventTestUtils() {}

    private static EventMetadata newEventMetadata(String id) {
        return new EventMetadata("requestId", id, Instant.EPOCH, "domain", "stream");
    }

    public static String queryEventJsonString() {
        return queryEventJsonString("id");
    }

    public static String queryEventJsonString(String id) {
        return "{" +
                "\"$schema\":\"/sparql/query/1.0.0\"," +
                "\"meta\":{" +
                "\"id\":\"" + id + "\"," +
                "\"dt\":\"1970-01-01T00:00:00Z\"," +
                "\"request_id\":\"requestId\"," +
                "\"domain\":\"domain\"," +
                "\"stream\":\"stream\"" +
                "}," +
                "\"http\":{" +
                "\"method\":\"GET\"," +
                "\"client_ip\":\"10.1.2.3\"," +
                "\"request_headers\":{\"header-name\":\"header-value\"}," +
                "\"has_cookies\":true," +
                "\"status_code\":200" +
                "}," +
                "\"backend_host\":\"backend_host\"," +
                "\"namespace\":\"namespace\"," +
                "\"query\":\"select ...\"," +
                "\"format\":\"json\"," +
                "\"params\":{\"param-name\":\"param-value\"}," +
                "\"query_time\":100}";

    }

    public static QueryEvent newQueryEvent() {
        return newQueryEvent("id");
    }

    public static QueryEvent newQueryEvent(String id) {
        Map<String, String> params = new HashMap<>();
        params.put("param-name", "param-value");
        return new QueryEvent(newEventMetadata(id), newEventHttpMetadata(), "backend_host",
                "namespace", "select ...", "json", params, Duration.ofMillis(100));
    }

    private static EventHttpMetadata newEventHttpMetadata() {
        Map<String, String> headers = new HashMap<>();
        headers.put("header-name", "header-value");
        return new EventHttpMetadata("GET", "10.1.2.3", headers, true, 200);
    }
}
