package org.wikidata.query.rdf.updater

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
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
    val kafkaBrokers: String = params.get("brokers")
    val pipelineInputEventStreamOptions = UpdaterPipelineInputEventStreamOptions(
      kafkaBrokers = kafkaBrokers,
      revisionCreateTopic = params.get("rev_create_topic"),
      consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
      maxLateness = params.getInt("max_lateness", 60000)
    )

    val checkpointDir = params.get("checkpoint_dir")
    val spuriousEventsDir = params.get("spurious_events_dir")
    val lateEventsDir = params.get("late_events_dir")
    val entityTriplesDir = params.get("entity_triples_dir")

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val uris: Uris = WikibaseRepository.Uris.fromString(s"https://$hostName")
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(UpdaterStateConfiguration.newStateBackend(checkpointDir))
    env.enableCheckpointing(2*60*1000) // checkpoint every 2mins, checkpoint timeout is 10m by default
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    UpdaterPipeline.build(pipelineOptions, buildIncomingStreams(pipelineInputEventStreamOptions, pipelineOptions, DEFAULT_CLOCK),
      rc => WikibaseEntityRevRepository(uris, rc.getMetricGroup),
      clock = DEFAULT_CLOCK)
      .saveLateEventsTo(prepareFileDebugSink(lateEventsDir))
      .saveSpuriousEventsTo(prepareFileDebugSink(spuriousEventsDir))
      .saveTo(prepareFileDebugSink(entityTriplesDir))
      .execute("WDQS Streaming Updater POC")
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
}

