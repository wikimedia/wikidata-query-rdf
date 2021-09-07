package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.SlidingWindowReservoir
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.Histogram
import org.apache.flink.metrics.groups.OperatorMetricGroup
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.EventsMeta

class MeasureEventProcessingLatencyOperationUnitTest extends FlatSpec with TestFixtures with BeforeAndAfter with MockFactory with Matchers {


  "MeasureEventProcessingLatencyOperation" should "register latency values" in {

    val instantNow: Instant = Instant.now()
    val eventProcessingMetricsOperation: MeasureEventProcessingLatencyOperation =
      MeasureEventProcessingLatencyOperation(Clock.fixed(instantNow, ZoneId.systemDefault()))
    val context: RuntimeContext = mock[RuntimeContext]
    val metricGroup: OperatorMetricGroup = mock[OperatorMetricGroup]
    val eventTimeHistogram = new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)))
    val processingTimeHistogram = new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500)))

    (metricGroup.histogram(_: String, _: Histogram)).expects("event-latency-ms", *).returning(eventTimeHistogram)
    (metricGroup.histogram(_: String, _: Histogram)).expects("processing-latency-ms", *).returning(processingTimeHistogram)
    (context.getMetricGroup _).expects().anyNumberOfTimes().returning(metricGroup)
    eventProcessingMetricsOperation.setRuntimeContext(context)
    eventProcessingMetricsOperation.open(new Configuration())

    val secondsBeforeCreated = 10L
    val secondsBeforeStartedProcessing = 5L
    val eventTime: Instant = instantNow.minusSeconds(secondsBeforeCreated)
    val processingTime: Instant = instantNow.minusSeconds(secondsBeforeStartedProcessing)
    val input: FullImport = FullImport("Q1", eventTime, 1, processingTime, newEventInfo(eventTime, "tested.domain", "tested.stream", "tested.request.id"))

    eventProcessingMetricsOperation.map(MutationDataChunk(input,
      new DiffEventData(new EventsMeta(clock.instant(), "", "", "", ""),
        "", 0, clock.instant(), 0, 1, MutationEventData.DIFF_OPERATION, new RDFDataChunk("", ""),
        new RDFDataChunk("", ""), new RDFDataChunk("", ""), new RDFDataChunk("", ""))))

    val eventTimeHisto: Array[Long] = eventTimeHistogram.getStatistics.getValues
    eventTimeHisto should have length 1
    eventTimeHisto(0) shouldBe TimeUnit.SECONDS.toMillis(secondsBeforeCreated)

    val processingTimeHisto: Array[Long] = processingTimeHistogram.getStatistics.getValues
    processingTimeHisto should have length 1
    processingTimeHisto(0) shouldBe TimeUnit.SECONDS.toMillis(secondsBeforeStartedProcessing)
  }
}
