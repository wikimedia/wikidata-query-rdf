package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{UpdaterConfig, UpdaterExecutionEnvironmentConfig, UpdaterPipelineOutputStreamConfig}

object UpdaterJob {
  val DEFAULT_CLOCK: Clock = Clock.systemUTC()

  def main(args: Array[String]): Unit = {
    val config = UpdaterConfig(args)
    val generalConfig = config.generalConfig

    val outputSink: SinkFunction[MutationDataChunk] = prepareKafkaSink(config.outputStreamConfig)
    val uris: Uris = WikibaseRepository.Uris.fromString(s"https://${generalConfig.hostname}")
    implicit val env: StreamExecutionEnvironment = prepareEnv(config.environmentConfig)

    val incomingStreams = IncomingStreams.buildIncomingStreams(config.InputEventStreamConfig, generalConfig.hostname, DEFAULT_CLOCK)
    UpdaterPipeline.build(generalConfig, incomingStreams,
      rc => WikibaseEntityRevRepository(uris, rc.getMetricGroup))
      .saveLateEventsTo(prepareErrorTrackingFileSink(config.lateEventsDir,
        InputEventEncoder.schema()), InputEventEncoder)
      .saveSpuriousEventsTo(prepareErrorTrackingFileSink(config.spuriousEventsDir,
        IgnoredMutationEncoder.schema()), IgnoredMutationEncoder)(new GenericRecordAvroTypeInfo(IgnoredMutationEncoder.schema()))
      .saveFailedOpsTo(prepareErrorTrackingFileSink(config.failedOpsDir, FailedOpEncoder.schema()),
        FailedOpEncoder)(new GenericRecordAvroTypeInfo(FailedOpEncoder.schema()))
      .saveTo(outputSink)
      .execute("WDQS Streaming Updater POC")
  }

  private def prepareEnv(environmentOption: UpdaterExecutionEnvironmentConfig): StreamExecutionEnvironment = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(UpdaterStateConfiguration.newStateBackend(environmentOption.checkpointDir))
    env.enableCheckpointing(environmentOption.checkpointInterval, environmentOption.checkpointingMode)
    env.getCheckpointConfig.setCheckpointTimeout(environmentOption.checkpointTimeout)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(environmentOption.minPauseBetweenCheckpoints)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    // FIXME Disable restarts for now, this is way easier to debug this way
    env.setRestartStrategy(new NoRestartStrategyConfiguration())
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getConfig.setAutoWatermarkInterval(environmentOption.autoWMInterval)
    env.setBufferTimeout(environmentOption.networkBufferTimeout)
    environmentOption.latencyTrackingInterval.foreach(l => env.getConfig.setLatencyTrackingInterval(l))
    env
  }

  private def prepareErrorTrackingFileSink(outputPath: String, schema: Schema): SinkFunction[GenericRecord] = {
    StreamingFileSink.forBulkFormat(new Path(outputPath), ParquetAvroWriters.forGenericRecord(schema))
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()
  }

  private def prepareKafkaSink(options: UpdaterPipelineOutputStreamConfig): SinkFunction[MutationDataChunk] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", options.kafkaBrokers)
    // Flink defaults is 1hour but wmf kafka uses the default value of 15min for transaction.max.timeout.ms
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("batch.size", "250000")
    producerConfig.setProperty("compression.type", "gzip")
    new FlinkKafkaProducer[MutationDataChunk](
      options.topic,
      new MutationEventDataSerializationSchema(options.topic, options.partition),
      producerConfig,
      options.checkpointingMode match {
        case CheckpointingMode.EXACTLY_ONCE => FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        case CheckpointingMode.AT_LEAST_ONCE => FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      })
  }
}

