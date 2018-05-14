package org.wikidata.query.rdf.blazegraph.throttling;

import static java.time.Instant.now;

import java.time.Clock;

import javax.servlet.http.HttpServletRequest;

import org.springframework.mock.web.MockHttpServletRequest;

public final class ThrottlingTestUtils {

    private ThrottlingTestUtils() {
        // utility class
    }

    static MockHttpServletRequest createRequest(String userAgent, String remoteAddr) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("User-Agent", userAgent);
        request.setRemoteAddr(remoteAddr);
        return request;
    }

    static boolean isThrottled(Throttler throttler, Object bucket, HttpServletRequest request, Clock clock) {
        return throttler.throttledUntil(bucket, request).isAfter(now(clock));
    }
}
