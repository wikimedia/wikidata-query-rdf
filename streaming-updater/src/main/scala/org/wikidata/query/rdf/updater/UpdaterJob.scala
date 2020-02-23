package org.wikidata.query.rdf.updater

import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.SinkFunction

object UpdaterJob {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // FIXME: proper options handling
    val pipelineOptions = UpdaterPipelineOptions(
      hostname = params.get("hostname"),
      reorderingWindowLengthMs = params.getInt("reordering_window_length", 60000)
    )
    val pipelineInputEventStreamOptions = UpdaterPipelineInputEventStreamOptions(
      kafkaBrokers = params.get("brokers"),
      revisionCreateTopic = params.get("rev_create_topic"),
      consumerGroup = params.get("consumer_group", "wdqs_streaming_updater"),
      maxLateness = params.getInt("max_lateness", 60000)
    )

    // FIXME: add external checkpoint config to handle job cancellation
    val checkpointDir = params.get("checkpoint_dir")
    val spuriousEventsDir = params.get("spurious_events_dir")
    val lateEventsDir = params.get("late_events_dir")
    val outputDir = params.get("output_dir")
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(new RocksDBStateBackend(checkpointDir))
    env.enableCheckpointing(30000)
    UpdaterPipeline.build(pipelineOptions, buildIncomingStreams(pipelineInputEventStreamOptions, pipelineOptions))
      .saveLateEventsTo(prepareJsonDebugSink(lateEventsDir))
      .saveSpuriousEventsTo(prepareJsonDebugSink(spuriousEventsDir))
      .saveTo(prepareJsonDebugSink(outputDir))
      .execute("WDQS Streaming Updater POC")
  }

  private def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamOptions,
                           opts: UpdaterPipelineOptions)
                          (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {
    List(
      IncomingStreams.fromKafka(
        KafkaConsumerProperties(ievops.revisionCreateTopic, ievops.kafkaBrokers, ievops.consumerGroup, new RevisionCreateEventJson()),
        opts.hostname,
        IncomingStreams.REV_CREATE_CONV,
        ievops.maxLateness
      )
    )
  }


  private def prepareJsonDebugSink[O](outputPath: String): SinkFunction[O] = {
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

