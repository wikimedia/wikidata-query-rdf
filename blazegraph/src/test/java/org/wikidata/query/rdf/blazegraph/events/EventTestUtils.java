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
                "\"$schema\":\"/sparql/query/1.3.0\"," +
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
                "\"graph_name\":\"my_graph\"," +
                "\"namespace\":\"namespace\"," +
                "\"query\":\"select ...\"," +
                "\"format\":\"json\"," +
                "\"params\":{\"param-name\":\"param-value\"}," +
                "\"query_time\":100," +
                "\"system_runtime_metrics\":{\"running_queries_before\":2,\"running_queries_after\":1,\"cpu_load_average\":1.0}," +
                "\"performer\":{\"user_text\":\"example_user\"}}";

    }

    public static QueryEvent newQueryEvent() {
        return newQueryEvent("id");
    }

    public static QueryEvent newQueryEvent(String id) {
        Map<String, String> params = new HashMap<>();
        params.put("param-name", "param-value");
        return new QueryEvent(newEventMetadata(id), newEventHttpMetadata(), "backend_host",
                "my_graph", "namespace", "select ...", "json", params, Duration.ofMillis(100),
                new SystemRuntimeMetrics(2, 1, 1d), new EventPerformer("example_user"));
    }

    private static EventHttpMetadata newEventHttpMetadata() {
        Map<String, String> headers = new HashMap<>();
        headers.put("header-name", "header-value");
        return new EventHttpMetadata("GET", "10.1.2.3", headers, true, 200);
    }
}
