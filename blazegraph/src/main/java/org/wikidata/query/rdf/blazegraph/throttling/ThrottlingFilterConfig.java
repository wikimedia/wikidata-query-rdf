package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

import java.time.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.FilterConfig;

/**
 * Load and parses configuration for the {@link ThrottlingFilter}.
 */
@ThreadSafe
public class ThrottlingFilterConfig {

    private final FilterConfig filterConfig;

    public ThrottlingFilterConfig(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    /**
     * Load a parameter from multiple locations.
     *
     * System properties have the highest priority, filter config is used if no
     * system property is found.
     *
     * The system property used is {@code wdqs.<filter-name>.<name>}.
     *
     * @param name name of the property
     * @param filterConfig used to get the filter config
     * @return the value of the parameter
     */
    private String loadStringParam(String name, FilterConfig filterConfig) {
        String result = null;
        String fParam = filterConfig.getInitParameter(name);
        if (fParam != null) {
            result = fParam;
        }
        String sParam = System.getProperty("wdqs." + filterConfig.getFilterName() + "." + name);
        if (sParam != null) {
            result = sParam;
        }
        return result;
    }

    /**
     * Load a parameter from multiple locations, with a default value.
     *
     * @see ThrottlingFilterConfig#loadStringParam(String, FilterConfig)
     * @param name
     * @param filterConfig
     * @param defaultValue
     * @return the parameter's value
     */
    private String loadStringParam(String name, FilterConfig filterConfig, String defaultValue) {
        return firstNonNull(loadStringParam(name, filterConfig), defaultValue);
    }

    /**
     * See {@link ThrottlingFilterConfig#loadStringParam(String, FilterConfig)}.
     *
     * @param name
     * @param filterConfig
     * @param defaultValue
     * @return
     */
    private boolean loadBooleanParam(String name, FilterConfig filterConfig, boolean defaultValue) {
        String result = loadStringParam(name, filterConfig);
        return result != null ? parseBoolean(result) : defaultValue;
    }

    /**
     * See {@link ThrottlingFilterConfig#loadStringParam(String, FilterConfig)}.
     *
     * @param name
     * @param filterConfig
     * @param defaultValue
     * @return
     */
    private int loadIntParam(String name, FilterConfig filterConfig, int defaultValue) {
        String result = loadStringParam(name, filterConfig);
        return result != null ? parseInt(result) : defaultValue;
    }

    public Duration getRequestDurationThreshold() {
        return Duration.ofMillis(loadIntParam("request-duration-threshold-in-millis", filterConfig, 0));
    }

    public Duration getTimeBucketCapacity() {
        return Duration.ofSeconds(loadIntParam("time-bucket-capacity-in-seconds", filterConfig, 120));
    }

    public Duration getTimeBucketRefillAmount() {
        return Duration.ofSeconds(loadIntParam("time-bucket-refill-amount-in-seconds", filterConfig, 60));
    }

    public Duration getTimeBucketRefillPeriod() {
        return Duration.ofMinutes(loadIntParam("time-bucket-refill-period-in-minutes", filterConfig, 1));
    }

    public int getErrorBucketCapacity() {
        return loadIntParam("error-bucket-capacity", filterConfig, 60);
    }

    public int getErrorBucketRefillAmount() {
        return loadIntParam("error-bucket-refill-amount", filterConfig, 30);
    }

    public Duration getErrorBucketRefillPeriod() {
        return Duration.ofMinutes(loadIntParam("error-bucket-refill-period-in-minutes", filterConfig, 1));
    }

    public int getThrottleBucketCapacity() {
        return loadIntParam("throttle-bucket-capacity", filterConfig, 200);
    }

    public int getThrottleBucketRefillAmount() {
        return loadIntParam("throttle-bucket-refill-amount", filterConfig, 200);
    }

    public Duration getThrottleBucketRefillPeriod() {
        return Duration.ofMinutes(loadIntParam("throttle-bucket-refill-period-in-minutes", filterConfig, 20));
    }

    public Duration getBanDuration() {
        return Duration.ofMinutes(loadIntParam("ban-duration-in-minutes", filterConfig, 60*24));
    }

    public int getMaxStateSize() {
        return loadIntParam("max-state-size", filterConfig, 10_000);
    }

    public Duration getStateExpiration() {
        return Duration.ofMinutes(loadIntParam("state-expiration-in-minutes", filterConfig, 15));
    }

    public String getEnableThrottlingIfHeader() {
        return loadStringParam("enable-throttling-if-header", filterConfig);
    }

    public String getEnableBanIfHeader() {
        return loadStringParam("enable-ban-if-header", filterConfig);
    }

    public String getAlwaysThrottleParam() {
        return loadStringParam("always-throttle-param", filterConfig, "throttleMe");
    }

    public String getAlwaysBanParam() {
        return loadStringParam("always-ban-param", filterConfig, "banMe");
    }

    public boolean isFilterEnabled() {
        return loadBooleanParam("enabled", filterConfig, true);
    }
}
