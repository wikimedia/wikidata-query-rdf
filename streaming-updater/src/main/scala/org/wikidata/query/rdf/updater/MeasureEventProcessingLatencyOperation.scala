package org.wikidata.query.rdf.updater

import java.time.Clock

import com.codahale.metrics.SlidingWindowReservoir
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Histogram

case class MeasureEventProcessingLatencyOperation(clock: Clock) extends RichMapFunction[ResolvedOp, ResolvedOp] {

  var eventLatencyHistogram: Histogram = _
  var processingLatencyHistogram: Histogram = _

  val EVENT_LATENCY_METRIC = "event-latency-ms"
  val PROCESSING_EVENT_METRIC = "processing-latency-ms"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    eventLatencyHistogram = getOrCreateHistogram(EVENT_LATENCY_METRIC)
    processingLatencyHistogram = getOrCreateHistogram(PROCESSING_EVENT_METRIC)
  }

  override def map(op: ResolvedOp): ResolvedOp = {
    val currentMillis: Long = clock.millis
    eventLatencyHistogram.update(currentMillis - op.operation.eventTime.toEpochMilli)
    processingLatencyHistogram.update(currentMillis - op.operation.ingestionTime.toEpochMilli)

    op
  }

  private def getOrCreateHistogram(metricName: String): DropwizardHistogramWrapper = {
    getRuntimeContext.getMetricGroup.histogram(metricName,
      new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))))
  }
}
