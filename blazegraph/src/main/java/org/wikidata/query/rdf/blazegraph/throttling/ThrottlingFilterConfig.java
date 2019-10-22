package org.wikidata.query.rdf.blazegraph.throttling;

import java.time.Duration;

import javax.annotation.concurrent.ThreadSafe;

import org.wikidata.query.rdf.blazegraph.filters.FilterConfiguration;

/**
 * Load and parses configuration for the {@link ThrottlingFilter}.
 */
@ThreadSafe
public class ThrottlingFilterConfig {

    /**
     * Default patterns list filename.
     */
    public static final String PATTERNS_DEFAULT = "patterns.txt";
    private final FilterConfiguration filterConfig;

    public ThrottlingFilterConfig(FilterConfiguration filterConfig) {
        this.filterConfig = filterConfig;
    }

    public Duration getRequestDurationThreshold() {
        return Duration.ofMillis(filterConfig.loadIntParam("request-duration-threshold-in-millis", 0));
    }

    public Duration getTimeBucketCapacity() {
        return Duration.ofSeconds(filterConfig.loadIntParam("time-bucket-capacity-in-seconds", 120));
    }

    public Duration getTimeBucketRefillAmount() {
        return Duration.ofSeconds(filterConfig.loadIntParam("time-bucket-refill-amount-in-seconds", 60));
    }

    public Duration getTimeBucketRefillPeriod() {
        return Duration.ofMinutes(filterConfig.loadIntParam("time-bucket-refill-period-in-minutes", 1));
    }

    public int getErrorBucketCapacity() {
        return filterConfig.loadIntParam("error-bucket-capacity", 60);
    }

    public int getErrorBucketRefillAmount() {
        return filterConfig.loadIntParam("error-bucket-refill-amount", 30);
    }

    public Duration getErrorBucketRefillPeriod() {
        return Duration.ofMinutes(filterConfig.loadIntParam("error-bucket-refill-period-in-minutes", 1));
    }

    public int getThrottleBucketCapacity() {
        return filterConfig.loadIntParam("throttle-bucket-capacity", 200);
    }

    public int getThrottleBucketRefillAmount() {
        return filterConfig.loadIntParam("throttle-bucket-refill-amount", 200);
    }

    public Duration getThrottleBucketRefillPeriod() {
        return Duration.ofMinutes(filterConfig.loadIntParam("throttle-bucket-refill-period-in-minutes", 20));
    }

    public Duration getBanDuration() {
        return Duration.ofMinutes(filterConfig.loadIntParam("ban-duration-in-minutes", 60*24));
    }

    public int getMaxStateSize() {
        return filterConfig.loadIntParam("max-state-size", 2000);
    }

    public Duration getStateExpiration() {
        return Duration.ofMinutes(filterConfig.loadIntParam("state-expiration-in-minutes", 15));
    }

    public String getEnableThrottlingIfHeader() {
        return filterConfig.loadStringParam("enable-throttling-if-header");
    }

    public String getEnableBanIfHeader() {
        return filterConfig.loadStringParam("enable-ban-if-header");
    }

    public String getAlwaysThrottleParam() {
        return filterConfig.loadStringParam("always-throttle-param", "throttleMe");
    }

    public String getAlwaysBanParam() {
        return filterConfig.loadStringParam("always-ban-param", "banMe");
    }

    public boolean isFilterEnabled() {
        return filterConfig.loadBooleanParam("enabled", true);
    }

    public String getRegexPatternsFile() {
        return filterConfig.loadStringParam("pattern-file", PATTERNS_DEFAULT);
    }

    public String getAgentPatternsFile() {
        return filterConfig.loadStringParam("agent-file", "agents.txt");
    }
}
