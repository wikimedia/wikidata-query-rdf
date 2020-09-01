package org.wikidata.query.rdf.updater;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;

public class DropwizardToFlinkListener implements MetricRegistryListener {

    private static final Logger LOG = LoggerFactory.getLogger(DropwizardToFlinkListener.class);

    private final MetricGroup metricGroup;

    public DropwizardToFlinkListener(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
        metricGroup.gauge(name, gauge::getValue);
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
        metricGroup.counter(name, new DropwizardCounterWrapper(counter));
    }

    @Override
    public void onHistogramAdded(String name, Histogram histogram) {
        metricGroup.histogram(name, new DropwizardHistogramWrapper(histogram));
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
        metricGroup.meter(name, new DropwizardMeterWrapper(meter));
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
        LOG.warn("Timer metric is not available for Apache Flink, dropwizard metric will be reported and " +
                "histogram metric will be used in Flink. Name: {}", name);

        TimerNanosecondsHistogram dropwizardHistogram = new TimerNanosecondsHistogram(timer);
        metricGroup.histogram(name + "(ns)", new DropwizardHistogramWrapper(dropwizardHistogram));
    }

    @Override
    public void onGaugeRemoved(String name) {
        logNotRemovedWarning(name);
    }

    @Override
    public void onCounterRemoved(String name) {
        logNotRemovedWarning(name);
    }

    @Override
    public void onHistogramRemoved(String name) {
        logNotRemovedWarning(name);
    }

    @Override
    public void onMeterRemoved(String name) {
        logNotRemovedWarning(name);
    }

    @Override
    public void onTimerRemoved(String name) {
        logNotRemovedWarning(name);
    }

    private void logNotRemovedWarning(String name) {
        LOG.warn("Removing metrics from Apache Flink's MetricGroup is unsupported. Metric {} was not removed.", name);
    }
}
