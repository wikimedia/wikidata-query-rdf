package org.wikidata.query.rdf.blazegraph.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import javax.servlet.http.Cookie;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.wikidata.query.rdf.test.ManualClock;

public class QueryEventGeneratorUnitTest {
    @Test
    public void testEventGeneration() {
        String namespace = "ns123";
        String pathInfo = "/namespace/" + namespace + "/sparql";
        String someId = "notunique";
        String requestId = "the_request_id";
        String hostname = "hostname.domain.local";
        String uri = "https://" + hostname + pathInfo;
        String format = "xml";
        String clientIp = "10.1.2.3";
        String method = "POST";
        String defaultNS = "defaultNS";
        int statusCode = 200;

        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        request.setParameter("foo", "bar");
        request.setParameter("baz", "bat", "bats,\\qux");
        request.setParameter(QueryEventGenerator.FORMAT_PARAM, format);
        request.setParameter(QueryEventGenerator.QUERY_PARAM, "select");

        request.addHeader("Accept", "application/json");
        request.addHeader("X-Request-Id", requestId);
        request.addHeader("X-Custom", "one");
        request.addHeader("X-Custom", "two,t\\hree");

        Supplier<String> uniqueIdGenerator = () -> someId;
        Clock clock = new ManualClock(Instant.EPOCH);

        request.setCookies(new Cookie[1]);
        request.setPathInfo(pathInfo);
        request.setRemoteAddr(clientIp);
        request.setServerName(hostname);

        QueryEventGenerator generator = new QueryEventGenerator(uniqueIdGenerator, clock, hostname);
        assertThat(generator.instant()).isEqualTo(Instant.EPOCH);
        QueryEvent event = generator.generateQueryEvent(request, statusCode, Duration.ofSeconds(1), Instant.EPOCH, defaultNS);
        assertThat(event.getBackendHost()).isEqualTo(hostname);
        assertThat(event.getFormat()).isEqualTo(format);
        assertThat(event.getNamespace()).isEqualTo(namespace);
        assertThat(event.getParams()).containsOnlyKeys("foo", "baz");
        assertThat(event.getParams()).containsValues("bar", "bat,bats\\,\\\\qux");
        assertThat(event.getQueryTime()).isEqualTo(Duration.ofSeconds(1).toMillis());
        assertThat(event.getMetadata().getDomain()).isEqualTo(hostname);
        assertThat(event.getMetadata().getDt()).isEqualTo(Instant.EPOCH);
        assertThat(event.getMetadata().getId()).isEqualTo(someId);
        assertThat(event.getMetadata().getRequestId()).isEqualTo(requestId);
        assertThat(event.getMetadata().getStream()).isEqualTo(QueryEventGenerator.STREAM_NAME);

        assertThat(event.getHttpMetadata().getClientIp()).isEqualTo(clientIp);
        assertThat(event.getHttpMetadata().getMethod()).isEqualTo(method);
        assertThat(event.getHttpMetadata().getRequestHeaders()).containsOnlyKeys("Accept", "X-Request-Id", "X-Custom");
        assertThat(event.getHttpMetadata().getRequestHeaders()).containsValues("application/json", requestId, "one,two\\,t\\\\hree");
        assertThat(event.getHttpMetadata().hasCookies()).isTrue();
        assertThat(event.getHttpMetadata().getStatusCode()).isEqualTo(statusCode);

        request.setPathInfo("/sparql");
        QueryEvent eventDefaultNS = generator.generateQueryEvent(request, statusCode, Duration.ofSeconds(1), Instant.EPOCH, defaultNS);
        assertThat(eventDefaultNS.getNamespace()).isEqualTo(defaultNS);
    }
}
