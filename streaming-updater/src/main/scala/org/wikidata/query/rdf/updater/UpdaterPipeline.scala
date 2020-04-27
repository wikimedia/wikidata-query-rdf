package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util.UUID

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

sealed class UpdaterPipeline(lateEventStream: DataStream[InputEvent],
                             spuriousEventStream: DataStream[IgnoredMutation],
                             tripleEventStream: DataStream[ResolvedOp])
                            (implicit env: StreamExecutionEnvironment)
{
  val defaultAppName = "WDQS Updater Stream Updater"

  def execute(appName: String = defaultAppName): Unit = {
    env.execute(appName)
  }

  def streamGraph(appName: String = defaultAppName): StreamGraph = {
    env.getStreamGraph(defaultAppName)
  }

  def saveLateEventsTo(sink: SinkFunction[InputEvent]): UpdaterPipeline = {
    lateEventStream.addSink(sink)
    this
  }

  def saveSpuriousEventsTo(sink: SinkFunction[IgnoredMutation]): UpdaterPipeline = {
    spuriousEventStream.addSink(sink)
    this
  }

  def saveTo(sink: SinkFunction[ResolvedOp]): UpdaterPipeline = {
    tripleEventStream.addSink(sink)
    this
  }
}

sealed case class UpdaterPipelineOptions (hostname: String,
                                          reorderingWindowLengthMs: Int
)

sealed case class UpdaterPipelineInputEventStreamOptions(kafkaBrokers: String,
                                                         consumerGroup: String,
                                                         revisionCreateTopic: String,
                                                         maxLateness: Int)

/**
 * Current state
 * stream1 = kafka(with periodic watermarks)
 *  => filter(domain == "something")
 *  => map(event convertion)
 * stream2 = same principle
 *
 * union of all streams
 *  => keyBy (used as a partitioner to reduce cardinality)
 *  => timeWindow(1min)
 *  => late events goes to LATE_EVENTS_SIDE_OUPUT_TAG
 *  => process(reorder the events within the window) see EventReordering
 *  => keyBy item
 *  => map(decide mutation ope) see DecideMutationOperation
 *  => process(remove spurious events) see RouteIgnoredMutationToSideOutput
 *  => flatMap(fetch data from wikibase and diff) see ExtractTriplesOperation
 *
 *  output of the stream a MutationOperation

 */
object UpdaterPipeline {
  def build(opts: UpdaterPipelineOptions, incomingStreams: List[DataStream[InputEvent]],
            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
            uniqueIdGenerator: () => String = UUID.randomUUID().toString,
            clock: Clock = Clock.systemUTC(),
            outputStreamName: String = "wdqs_streaming_updater")
           (implicit env: StreamExecutionEnvironment): UpdaterPipeline = {
    val incomingEventStream: DataStream[InputEvent] = incomingStreams match {
      case Nil => throw new NoSuchElementException("at least one stream is needed")
      case x :: Nil => x
      case x :: rest => x.union(rest: _*)
    }
    val windowStream: DataStream[InputEvent] = EventReorderingWindowFunction.attach(incomingEventStream, Time.milliseconds(opts.reorderingWindowLengthMs))
    val lateEventsSideOutput: DataStream[InputEvent] = windowStream.getSideOutput(EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG)

    val outputMutationStream: DataStream[MutationOperation] = windowStream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .name("DecideMutationOperation")
      .uid("DecideMutationOperation")
      .process(new RouteIgnoredMutationToSideOutput())
      .name("output")
      .uid("output")

    val spuriousEventsLate: DataStream[IgnoredMutation] = outputMutationStream.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS)

    val tripleEventStream: DataStream[ResolvedOp] = outputMutationStream
      .flatMap(GenerateEntityDiffPatchOperation(
        domain = opts.hostname,
        wikibaseRepositoryGenerator = wikibaseRepositoryGenerator,
        uniqueIdGenerator = uniqueIdGenerator,
        clock = clock,
        stream = outputStreamName)
      )
      .name("ExtractTriplesOperation")
      .uid("ExtractTriplesOperation")
      .map(MeasureEventProcessingLatencyOperation(clock))
      .name("MeasureEventProcessingLatencyOperation")
      .uid("MeasureEventProcessingLatencyOperation")

    new UpdaterPipeline(lateEventsSideOutput, spuriousEventsLate, tripleEventStream)
  }
}
