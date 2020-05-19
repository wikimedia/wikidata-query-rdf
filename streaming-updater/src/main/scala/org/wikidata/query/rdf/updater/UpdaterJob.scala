package org.wikidata.query.rdf.updater

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.util.concurrent.TimeUnit
import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris

object UpdaterJob {
  val DEFAULT_CLOCK = Clock.systemUTC()
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    val hostName: String = params.get("hostname")

    // FIXME: proper options handling
    val pipelineOptions = UpdaterPipelineOptions(
      hostname = hostName,
      reorderingWindowLengthMs = params.getInt("reordering_window_length", 60000)
    )
    val inputKafkaBrokers: String = params.get("brokers")
    val outputKafkaBrokers: String = params.get("output_brokers", inputKafkaBrokers)
    val outputTopic: String = params.get("output_topic")
    val outputPartition: Int = params.getInt("output_topic_partition")

    val pipelineInputEventStreamOptions = UpdaterPipelineInputEventStreamOptions(
      kafkaBrokers = inputKafkaBrokers,
      revisionCreateTopic = params.get("rev_create_topic"),
      consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
      maxLateness = params.getInt("max_lateness", 60000)
    )

    val checkpointDir: String = params.get("checkpoint_dir")
    val spuriousEventsDir: String = params.get("spurious_events_dir")
    val failedOpsDir: String = params.get("failed_ops_dir")
    val lateEventsDir: String = params.get("late_events_dir")

    val outputSink: SinkFunction[EntityPatchOp] = prepareKafkaSink(UpdaterPipelineOutputStreamOption(outputKafkaBrokers, outputTopic, outputPartition))
    val outputSinkParallelism = Some(1)

    val uris: Uris = WikibaseRepository.Uris.fromString(s"https://$hostName")
    implicit val env: StreamExecutionEnvironment = prepareEnv(checkpointDir)
    UpdaterPipeline.build(pipelineOptions, buildIncomingStreams(pipelineInputEventStreamOptions, pipelineOptions, DEFAULT_CLOCK),
      rc => WikibaseEntityRevRepository(uris, rc.getMetricGroup),
      clock = DEFAULT_CLOCK)

    UpdaterPipeline.build(pipelineOptions, buildIncomingStreams(pipelineInputEventStreamOptions, pipelineOptions, clock = DEFAULT_CLOCK),
      rc => WikibaseEntityRevRepository(uris, rc.getMetricGroup))
      .saveLateEventsTo(prepareFileDebugSink(lateEventsDir))
      .saveSpuriousEventsTo(prepareFileDebugSink(spuriousEventsDir))
      .saveFailedOpsTo(prepareFileDebugSink(failedOpsDir))
      .saveTo(outputSink, outputSinkParallelism)
      .execute("WDQS Streaming Updater POC")
  }

  private def prepareEnv(checkpointDir: String): StreamExecutionEnvironment = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(UpdaterStateConfiguration.newStateBackend(checkpointDir))
    env.enableCheckpointing(20 * 1000) // checkpoint every 20secs, checkpoint timeout is 10m by default
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(0)
    // Disable restarts for now, this is way easier to debug this way
    env.setRestartStrategy(new NoRestartStrategyConfiguration())
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env
  }

  private def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamOptions,
                                   opts: UpdaterPipelineOptions, clock: Clock)
                                  (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {
    List(
      IncomingStreams.fromKafka(
        KafkaConsumerProperties(ievops.revisionCreateTopic, ievops.kafkaBrokers, ievops.consumerGroup, new RevisionCreateEventJson()),
        opts.hostname,
        IncomingStreams.REV_CREATE_CONV,
        ievops.maxLateness,
        clock
      )
    )
  }


  private def prepareFileDebugSink[O](outputPath: String): SinkFunction[O] = {
    StreamingFileSink.forRowFormat(new Path(outputPath),
      new Encoder[O] {
        override def encode(element: O, stream: OutputStream): Unit = {
          stream.write(s"$element\n".getBytes(StandardCharsets.UTF_8))
        }
      })
      .withRollingPolicy(DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build())
      .build()
  }

  private def prepareKafkaSink(options: UpdaterPipelineOutputStreamOption): SinkFunction[EntityPatchOp] = {
    val producerConfig = new Properties()
    producerConfig.setProperty("bootstrap.servers", options.kafkaBrokers)
    producerConfig.setProperty("transaction.timeout.ms", "900000")
    producerConfig.setProperty("timeout.ms", "900000")
    producerConfig.setProperty("delivery.timeout.ms", "900000")
    producerConfig.setProperty("batch.size", "250000")
    producerConfig.setProperty("linger.ms", "1")
    producerConfig.setProperty("compression.type", "gzip")
    new FlinkKafkaProducer[EntityPatchOp](
      options.topic,
      new MutationEventDataSerializationSchema(options.topic, options.partition),
      producerConfig,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
  }
}

