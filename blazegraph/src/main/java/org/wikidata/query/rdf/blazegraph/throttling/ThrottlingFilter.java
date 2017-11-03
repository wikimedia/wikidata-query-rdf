package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.isomorphism.util.TokenBuckets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.throttling.UserAgentIpAddressBucketing.Bucket;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;

/**
 * A Servlet Filter that applies throttling.
 *
 * The throttling is based on the request time consumed and the number of
 * errors. The rational is:
 *
 * <dl>
 *     <dt>request time</dt>
 *     <dd>This is a good proxy for how much resource (CPU, IO) are consumed.</dd>
 *     <dt>errors</dt>
 *     <dd>A client always in error indicates a problem client side, which
 *     should be fixed client side.</dd>
 * </dl>
 *
 * Resource consumption is based on <a
 * href="https://en.wikipedia.org/wiki/Token_bucket">token buckets</a> as
 * implemented by <a href="https://github.com/bbeck/token-bucket">bbeck</a>. A
 * token bucket is defined by:
 *
 * <dl>
 *     <dt>capacity</dt>
 *     <dd>the maximum number of tokes in the bucket</dd>
 *     <dt>refill amount</dt>
 *     <dd>the number of tokens to add to the bucket when refilling</dd>
 *     <dt>refill period</dt>
 *     <dd>how often to refill the bucket</dd>
 * </dl>
 *
 * This filter has two buckets, one to keep track of time, and one to keep
 * track of errors. Each time an error occurs, a token is taken out of the
 * error bucket. Each refill period, tokens are added again. The time bucket
 * has a similar behaviour. As an optimization, we start keeping track of
 * resource consumption only if:
 *
 * <ol>
 *     <li>a request is taking a significant time</li>
 *     <li>a request is in error</li>
 * </ol>
 *
 * The client is throttled if either the time bucket or the error bucket is
 * empty. Since we don't know in advance the cost of a request or if it is
 * going to be in error, the throttling will only occur for the next requests.
 *
 * In case of throttling, the client is notified by an HTTP 429 status code and
 * is presented with a <code>Retry-After</code> HTTP header giving a backoff
 * time in seconds.
 *
 * The clients are segmented in different buckets and resource consumption is
 * tracked individually for each of those buckets. The segmentation is done by
 * [IP address, User Agent], but could be extended to support more complex
 * strategies. A bucket is only kept while its client is active. After a period
 * of inactivity, the bucket is deleted.
 *
 * All state is limited to a single JVM, this filter is not cluster aware.
 */
public class ThrottlingFilter implements Filter, ThrottlingMXBean {

    private static final Logger log = LoggerFactory.getLogger(ThrottlingFilter.class);

    /** Is throttling enabled. */
    private boolean enabled;
    /** To delegate throttling logic. */
    private Throttler<Bucket> throttler;

    /** Keeps track of the number of requests being throttled. */
    private final AtomicLong nbThrottledRequests = new AtomicLong();

    /** The object name under which the stats for this filter are exposed through JMX. */
    @Nullable  private ObjectName objectName;

    /**
     * Initialise the filter.
     *
     * See {@link ThrottlingFilter#loadStringParam(String, FilterConfig)} for
     * the details of where the configuration is loaded from.
     *
     * @param filterConfig {@inheritDoc}
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        int requestDurationThresholdInMillis = loadIntParam("request-duration-threshold-in-millis", filterConfig, 0);
        int timeBucketCapacityInSeconds = loadIntParam("time-bucket-capacity-in-seconds", filterConfig, 120);
        int timeBucketRefillAmountInSeconds = loadIntParam("time-bucket-refill-amount-in-seconds", filterConfig, 60);
        int timeBucketRefillPeriodInMinutes = loadIntParam("time-bucket-refill-period-in-minutes", filterConfig, 1);
        int errorBucketCapacity = loadIntParam("error-bucket-capacity", filterConfig, 60);
        int errorBucketRefillAmount = loadIntParam("error-bucket-refill-amount", filterConfig, 30);
        int errorBucketRefillPeriodInMinutes = loadIntParam("error-bucket-refill-period-in-minutes", filterConfig, 1);
        int maxStateSize = loadIntParam("max-state-size", filterConfig, 10000);
        int stateExpirationInMinutes = loadIntParam("state-expiration-in-minutes", filterConfig, 15);

        String enableThrottlingIfHeader = loadStringParam("enable-throttling-if-header", filterConfig);
        String alwaysThrottleParam = loadStringParam("always-throttle-param", filterConfig, "throttleMe");

        this.enabled = loadBooleanParam("enabled", filterConfig, true);
        throttler = new Throttler<>(
                Duration.of(requestDurationThresholdInMillis, MILLIS),
                new UserAgentIpAddressBucketing(),
                createThrottlingState(
                        timeBucketCapacityInSeconds,
                        timeBucketRefillAmountInSeconds,
                        timeBucketRefillPeriodInMinutes,
                        errorBucketCapacity,
                        errorBucketRefillAmount,
                        errorBucketRefillPeriodInMinutes),
                CacheBuilder.newBuilder()
                        .maximumSize(maxStateSize)
                        .expireAfterAccess(stateExpirationInMinutes, TimeUnit.MINUTES)
                        .build(),
                enableThrottlingIfHeader,
                alwaysThrottleParam);

        registerMBean(filterConfig.getFilterName());
    }

    /**
     * Register this Filter as an MBean.
     *
     * On successful registration, the {@link ObjectName} used for registration
     * will be stored into the instance field {@link ThrottlingFilter#objectName}.
     *
     * @param filterName
     */
    private void registerMBean(String filterName) {
        try {
            ObjectName objectName = new ObjectName(ThrottlingFilter.class.getName(), "filterName", filterName);
            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            platformMBeanServer.registerMBean(this, objectName);
            this.objectName = objectName;
            log.info("ThrottlingFilter MBean registered as {}.", objectName);
        } catch (MalformedObjectNameException e) {
            log.error("filter name {} is invalid as an MBean property.", filterName, e);
        } catch (InstanceAlreadyExistsException e) {
            log.error("MBean for ThrottlingFilter has already been registered.", e);
        } catch (NotCompliantMBeanException | MBeanRegistrationException e) {
            log.error("Could not register MBean for ThrottlingFilter.", e);
        }
    }

    /**
     * See {@link ThrottlingFilter#loadStringParam(String, FilterConfig)}.
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

    /**
     * See {@link ThrottlingFilter#loadStringParam(String, FilterConfig)}.
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
     * Load a parameter from multiple locations.
     *
     * System properties have the highest priority, filter config is used if no
     * system property is found.
     *
     * The system property used is <code>wdqs.&lt;filter-name&gt;.&lt;name&gt;</code>.
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
     * @see ThrottlingFilter#loadStringParam(String, FilterConfig)
     * @param name
     * @param filterConfig
     * @param defaultValue
     * @return the parameter's value
     */
    private String loadStringParam(String name, FilterConfig filterConfig, String defaultValue) {
        return firstNonNull(loadStringParam(name, filterConfig), defaultValue);
    }

    /**
     * Create Callable to initialize throttling state.
     *
     * @param timeBucketCapacityInSeconds
     * @param timeBucketRefillAmountInSeconds
     * @param timeBucketRefillPeriodInMinutes
     * @param errorBucketCapacity
     * @param errorBucketRefillAmount
     * @param errorBucketRefillPeriodInMinutes
     */
    public static Callable<ThrottlingState> createThrottlingState(
            int timeBucketCapacityInSeconds,
            int timeBucketRefillAmountInSeconds,
            int timeBucketRefillPeriodInMinutes,
            int errorBucketCapacity,
            int errorBucketRefillAmount,
            int errorBucketRefillPeriodInMinutes) {
        return () -> new ThrottlingState(
                TokenBuckets.builder()
                        .withCapacity(
                                MILLISECONDS.convert(timeBucketCapacityInSeconds, TimeUnit.SECONDS))
                        .withFixedIntervalRefillStrategy(
                                MILLISECONDS.convert(timeBucketRefillAmountInSeconds, TimeUnit.SECONDS),
                                timeBucketRefillPeriodInMinutes, MINUTES)
                        .build(),
                TokenBuckets.builder()
                        .withCapacity(errorBucketCapacity)
                        .withFixedIntervalRefillStrategy(
                                errorBucketRefillAmount,
                                errorBucketRefillPeriodInMinutes, MINUTES)
                        .build());
    }

    /**
     * Check resource consumption and throttle requests as needed.
     *
     * @param request {@inheritDoc}
     * @param response {@inheritDoc}
     * @param chain {@inheritDoc}
     * @throws IOException {@inheritDoc}
     * @throws ServletException {@inheritDoc}
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (throttler.isThrottled(httpRequest)) {
            log.info("A request is being throttled.");
            if (enabled) {
                nbThrottledRequests.incrementAndGet();
                notifyUser(httpResponse, throttler.getBackoffDelay(httpRequest));
                return;
            }
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            chain.doFilter(request, response);
            // for throttling purpose, consider all 1xx and 2xx status codes as
            // success, 4xx and 5xx as failure
            if (httpResponse.getStatus() < 400) {
                throttler.success(httpRequest, stopwatch.elapsed());
            } else {
                throttler.failure(httpRequest, stopwatch.elapsed());
            }
        } catch (IOException | ServletException e) {
            // an exception processing the request is treated as a failure
            throttler.failure(httpRequest, stopwatch.elapsed());
            throw e;
        }
    }

    /**
     * Notify the user that he is being throttled.
     *
     * @param response the response
     * @param backoffDelay the backoff delay
     * @throws IOException if the response cannot be written
     */
    private void notifyUser(HttpServletResponse response, Duration backoffDelay) throws IOException {
        String retryAfter = Long.toString(backoffDelay.getSeconds());
        response.setHeader("Retry-After", retryAfter);
        response.sendError(429, format(ENGLISH, "Too Many Requests - Please retry in %s seconds.", retryAfter));
    }

    /** Unregister MBean. */
    @Override
    public void destroy() {
        // Don't do anything if the MBean isn't registered.
        if (objectName == null) return;

        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            platformMBeanServer.unregisterMBean(objectName);
            log.info("ThrottlingFilter MBean {} unregistered.", objectName);
            objectName = null;
        } catch (InstanceNotFoundException e) {
            log.warn("MBean already unregistered.", e);
        } catch (MBeanRegistrationException e) {
            log.error("Could not unregister MBean.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long getStateSize() {
        return throttler.getStateSize();
    }

    /** {@inheritDoc} */
    @Override
    public long getNumberOfThrottledRequests() {
        return nbThrottledRequests.get();
    }

}
