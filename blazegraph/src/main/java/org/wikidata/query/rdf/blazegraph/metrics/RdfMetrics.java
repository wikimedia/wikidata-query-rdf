package org.wikidata.query.rdf.blazegraph.metrics;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

/**
 * Service Locator used as an entry point into metrics.
 *
 * There is probably a better way to do this in Blazegraph.
 */
@ThreadSafe
public final class RdfMetrics implements ServletContextListener {

    public static final MetricRegistry METRICS_REGISTRY = new MetricRegistry();
    private JmxReporter jmxReporter;


    private synchronized void configure(MetricRegistry registry) {
        jmxReporter = JmxReporter.forRegistry(registry).build();
        jmxReporter.start();
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        configure(METRICS_REGISTRY);
    }

    @Override
    public synchronized void contextDestroyed(ServletContextEvent sce) {
        jmxReporter.stop();
    }
}
