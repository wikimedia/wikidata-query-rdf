package org.wikidata.query.rdf.common.log;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.common.log.LoggingFixtures.logEvent;

import org.junit.Test;

public class RateLimitFilterUnitTest {

    @Test
    public void logNotThrottledWhenUnderLimit() {
        RateLimitFilter filter = createRateLimitFilter();
        for (int i = 0; i < 3; i++) {
            assertThat(filter.decide(logEvent())).isEqualTo(NEUTRAL);
        }
    }

    @Test
    public void logThrottledWhenOverLimit() {
        RateLimitFilter filter = createRateLimitFilter();
        for (int i = 0; i < 3; i++) {
            filter.decide(logEvent());
        }
        assertThat(filter.decide(logEvent())).isEqualTo(DENY);
    }

    @Test
    public void logFlowingAgainAfterRefillDuration() throws InterruptedException {
        RateLimitFilter filter = createRateLimitFilter();
        for (int i = 0; i < 3; i++) {
            filter.decide(logEvent());
        }
        Thread.sleep(51);
        assertThat(filter.decide(logEvent())).isEqualTo(NEUTRAL);
    }

    private RateLimitFilter createRateLimitFilter() {
        RateLimitFilter rateLimitFilter = new RateLimitFilter();
        rateLimitFilter.setBucketCapacity(3);
        rateLimitFilter.setRefillIntervalInMillis(50);
        return rateLimitFilter;
    }

}
