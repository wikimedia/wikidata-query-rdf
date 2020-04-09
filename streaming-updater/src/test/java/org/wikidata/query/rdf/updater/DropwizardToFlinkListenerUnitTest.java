package org.wikidata.query.rdf.updater;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.metrics.MetricGroup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

@RunWith(MockitoJUnitRunner.class)
public class DropwizardToFlinkListenerUnitTest {

    @Mock
    MetricGroup metricGroupMock;

    @Captor
    ArgumentCaptor<org.apache.flink.metrics.Gauge<Integer>> gaugeArgumentCaptor;

    @Captor
    ArgumentCaptor<org.apache.flink.metrics.Counter> counterArgumentCaptor;

    @Captor
    ArgumentCaptor<org.apache.flink.metrics.Meter> meterArgumentCaptor;

    @Captor
    ArgumentCaptor<org.apache.flink.metrics.Histogram> histogramArgumentCaptor;

    private MetricRegistry metricRegistry;


    @Before
    public void setUp() {
        metricRegistry = new MetricRegistry();
        metricRegistry.addListener(new DropwizardToFlinkListener(metricGroupMock));
    }

    @Test
    public void shouldRegisterGauges() {
        AtomicInteger value = new AtomicInteger(0);
        Gauge<Integer> integerGauge = value::intValue;
        int gaugeValue = 10;
        String gaugeName = "gauge";
        metricRegistry.gauge(gaugeName, () -> integerGauge);

        value.set(gaugeValue);
        verify(metricGroupMock).gauge(eq(gaugeName), gaugeArgumentCaptor.capture());
        org.apache.flink.metrics.Gauge<Integer> flinkGauge = gaugeArgumentCaptor.getValue();
        assertThat(flinkGauge.getValue()).isEqualTo(gaugeValue);
    }

    @Test
    public void shouldRegisterCounters() {
        String counterName = "counter";
        Counter counter = metricRegistry.counter(counterName);
        int counterCount = 5;
        counter.inc(counterCount);

        verify(metricGroupMock).counter(eq(counterName), counterArgumentCaptor.capture());
        assertThat(counterArgumentCaptor.getValue().getCount()).isEqualTo(counterCount);
    }

    @Test
    public void shouldRegisterMeters() {
        String meterName = "meter";
        Meter meter = metricRegistry.meter(meterName);
        int markedTimes = 4;
        meter.mark(markedTimes);

        verify(metricGroupMock).meter(eq(meterName), meterArgumentCaptor.capture());
        assertThat(meterArgumentCaptor.getValue().getCount()).isEqualTo(markedTimes);
    }

    @Test
    public void shouldRegisterHistograms() {
        String histogramName = "histogram";
        Histogram histogram = metricRegistry.histogram(histogramName);
        int updateValue = 10;
        histogram.update(updateValue);
        histogram.update(2 * updateValue);

        verify(metricGroupMock).histogram(eq(histogramName), histogramArgumentCaptor.capture());
        org.apache.flink.metrics.Histogram flinkHistogram = histogramArgumentCaptor.getValue();
        assertThat(flinkHistogram.getCount()).isEqualTo(2);
        assertThat(flinkHistogram.getStatistics().getValues()).containsExactly(updateValue, 2 * updateValue);
    }

    @Test
    public void shouldRegisterTimersAsMetersAndHistograms() {
        String timerName = "timer";
        Timer timer = metricRegistry.timer(timerName);

        int duration = 10;
        timer.update(duration, TimeUnit.SECONDS);
        timer.update(2 * duration, TimeUnit.SECONDS);

        verify(metricGroupMock).histogram(eq(timerName + "(ns)"), histogramArgumentCaptor.capture());
        org.apache.flink.metrics.Histogram histogram = histogramArgumentCaptor.getValue();
        assertThat(histogram.getCount()).isEqualTo(2);
        assertThat(histogram.getStatistics().getValues())
                .containsExactly(TimeUnit.SECONDS.toNanos(duration),
                                 TimeUnit.SECONDS.toNanos(2 * duration));
    }
}
