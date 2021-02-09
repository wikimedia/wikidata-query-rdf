package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant}
import java.util.Properties
import java.util.function.Supplier

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, SinkFunction}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.wikidata.query.rdf.updater.config.UpdaterPipelineOutputStreamConfig
import org.wikimedia.eventutilities.core.event.{EventStreamConfig, JsonEventGenerator}

case class OutputStreams(
                          mutationSink: SinkFunction[MutationDataChunk],
                          lateEventsSink: SinkFunction[InputEvent] = new DiscardingSink[InputEvent],
                          spuriousEventsSink: SinkFunction[IgnoredMutation] = new DiscardingSink[IgnoredMutation],
                          failedOpsSink: SinkFunction[FailedOp] = new DiscardingSink[FailedOp]
                        )

class OutputStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig) {
  private val eventStreamConfig: EventStreamConfig = EventStreamConfig.builder()
    .setEventStreamConfigLoader(outputStreamsConfig.eventStreamConfigEndpoint)
    .build()
  private val clock = new Supplier[Instant] {
    private val utcClock = Clock.systemUTC()
    override def get(): Instant = utcClock.instant()
  }
  private val jsonEventGenerator: JsonEventGenerator = JsonEventGenerator.builder()
    .eventStreamConfig(eventStreamConfig)
    .ingestionTimeClock(clock)
    .build()
  private val jsonEncoders = new JsonEncoders(jsonEventGenerator, outputStreamsConfig.sideOutputsDomain)

  def lateEventsOutput: SinkFunction[InputEvent] = {
    outputStreamsConfig.lateEventOutputDir match {
      case Some(dir) => wrapGenericRecordSinkFunction(prepareErrorTrackingFileSink(dir, InputEventEncoder.schema()), InputEventEncoder.map)
      case _ => prepareSideOutputStream[InputEvent](JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema)
    }
  }

  def spuriousEventsOutput: SinkFunction[IgnoredMutation] = {
    outputStreamsConfig.spuriousEventOutputDir match {
      case Some(dir) => wrapGenericRecordSinkFunction(prepareErrorTrackingFileSink(dir, IgnoredMutationEncoder.schema()), IgnoredMutationEncoder.map)
      case _ => prepareSideOutputStream[IgnoredMutation](JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema)
    }
  }

  def failedOpOutput: SinkFunction[FailedOp] = {
    outputStreamsConfig.failedEventOutputDir match {
      case Some(dir) => wrapGenericRecordSinkFunction(prepareErrorTrackingFileSink(dir, FailedOpEncoder.schema()), FailedOpEncoder.map)
      case _ => prepareSideOutputStream[FailedOp](JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema)
    }
  }

  def build: OutputStreams = {
    OutputStreams(mutationOutput, lateEventsOutput, spuriousEventsOutput, failedOpOutput)
  }

  private def prepareSideOutputStream[E](stream: String, schema: String): SinkFunction[E] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers))
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    new FlinkKafkaProducer[E](topic, jsonEncoders.getSerializationSchema[E](topic, stream, schema, clock),
      producerConfig,
      // force at least once semantic (WMF event platform does not seem to support kafka transactions yet)
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
  }

  def mutationOutput: SinkFunction[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", outputStreamsConfig.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("batch.size", "250000")
    producerConfig.setProperty("compression.type", "gzip")
    new FlinkKafkaProducer[MutationDataChunk](
      outputStreamsConfig.topic,
      new MutationEventDataSerializationSchema(outputStreamsConfig.topic, outputStreamsConfig.partition),
      producerConfig,
      outputStreamsConfig.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      })
  }

  private def wrapGenericRecordSinkFunction[I](sinkFunction: SinkFunction[GenericRecord], mapper: I => GenericRecord): SinkFunction[I] = {
    new SinkFunction[I] {
      override def invoke(value: I, ctx: SinkFunction.Context): Unit = sinkFunction.invoke(mapper.apply(value), ctx)
    }
  }

  private def prepareErrorTrackingFileSink(outputPath: String, schema: Schema): SinkFunction[GenericRecord] = {
    StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forGenericRecord(schema))
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()
  }
}
