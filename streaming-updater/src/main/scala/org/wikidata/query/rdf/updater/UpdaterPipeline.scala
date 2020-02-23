package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

sealed class UpdaterPipeline(outputStream: DataStream[MutationOperation],
                             lateEventStream: DataStream[InputEvent],
                             spuriousEventStream: DataStream[IgnoredMutation])
                            (implicit env: StreamExecutionEnvironment)
{
  def execute(appName: String = "WDQS Updater Stream Updater"): Unit = {
    env.execute(appName)
  }

  def saveLateEventsTo(sink: SinkFunction[InputEvent]): UpdaterPipeline = {
    lateEventStream.addSink(sink)
    this
  }

  def saveSpuriousEventsTo(sink: SinkFunction[IgnoredMutation]): UpdaterPipeline = {
    spuriousEventStream.addSink(sink)
    this
  }

  def saveTo(sink: SinkFunction[MutationOperation]): UpdaterPipeline = {
    outputStream.addSink(sink)
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
 *
 *  output of the stream a MutationOperation
 *  TODO: fetch info from Wikibase, generate the diff, output triples
 */
object UpdaterPipeline {
  def build(opts: UpdaterPipelineOptions, incomingStreams: List[DataStream[InputEvent]])(implicit env: StreamExecutionEnvironment): UpdaterPipeline = {
    val incomingEventStream = incomingStreams match {
      case Nil => throw new NoSuchElementException("at least one stream is needed")
      case x :: Nil => x
      case x :: rest => x.union(rest: _*)
    }

    val windowStream = EventReorderingWindowFunction.attach(incomingEventStream)
    val lateEventsSideOutput = windowStream.getSideOutput(EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG)
    val outputEventStream = windowStream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .uid("DecideMutationOperation")
      .process(new RouteIgnoredMutationToSideOutput())
      .name("output")
      .uid("output")
    val spuriousEventsLate = outputEventStream.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS)
    new UpdaterPipeline(outputEventStream, lateEventsSideOutput, spuriousEventsLate)
  }
}
