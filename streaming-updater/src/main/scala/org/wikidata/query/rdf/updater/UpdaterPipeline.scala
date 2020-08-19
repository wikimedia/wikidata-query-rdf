package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util.UUID

import scala.concurrent.duration.MILLISECONDS
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.wikidata.query.rdf.tool.rdf.Patch

sealed class UpdaterPipeline(lateEventStream: DataStream[InputEvent],
                             spuriousEventStream: DataStream[IgnoredMutation],
                             failedOpsStream: DataStream[FailedOp],
                             tripleEventStream: DataStream[MutationDataChunk],
                             updaterPipelineOptions: UpdaterPipelineOptions
                            )
                            (implicit env: StreamExecutionEnvironment)
{
  val defaultAppName = "WDQS Updater Stream Updater"

  def execute(appName: String = defaultAppName): Unit = {
    env.execute(appName)
  }

  def streamGraph(appName: String = defaultAppName): StreamGraph = {
    env.getStreamGraph(appName)
  }

  def saveLateEventsTo(sink: SinkFunction[InputEvent]): UpdaterPipeline = {
    lateEventStream.addSink(sink)
      .uid("late-events-output")
      .name("late-events-output")
    this
  }

  def saveSpuriousEventsTo(sink: SinkFunction[IgnoredMutation]): UpdaterPipeline = {
    spuriousEventStream.addSink(sink)
      .uid("spurious-events-output")
      .name("spurious-events-output")
    this
  }

  def saveFailedOpsTo(sink: SinkFunction[FailedOp]): UpdaterPipeline = {
    failedOpsStream.addSink(sink)
      .uid("failed-ops-output")
      .name("failed-ops-output")
    this
  }

  def saveTo(sink: SinkFunction[MutationDataChunk]): UpdaterPipeline = {
    tripleEventStream.addSink(sink)
      .uid("output")
      .name("output")
      .setParallelism(updaterPipelineOptions.outputParallelism)
    this
  }
}

sealed case class UpdaterPipelineOptions(hostname: String,
                                         reorderingWindowLengthMs: Int,
                                         reorderingOpParallelism: Option[Int],
                                         decideMutationOpParallelism: Option[Int],
                                         generateDiffParallelism: Int,
                                         generateDiffTimeout: Long,
                                         wikibaseRepoThreadPoolSize: Int,
                                         outputParallelism: Int = 1
)

sealed case class UpdaterPipelineInputEventStreamOptions(kafkaBrokers: String,
                                                         consumerGroup: String,
                                                         revisionCreateTopicName: String,
                                                         pageDeleteTopicName: String,
                                                         maxLateness: Int)

sealed case class UpdaterPipelineOutputStreamOption(
                                                   kafkaBrokers: String,
                                                   topic: String,
                                                   partition: Int,
                                                   checkpointingMode: CheckpointingMode
                                                   )

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
 *  => map(fetch data from wikibase and diff) see GenerateEntityDiffPatchOperation
 *  => process(remove failed ops) see RouteFailedOpsToSideOutput
 *  => flatMap(split large patches into chunks) see RDFPatchChunkOperation
 *  output of the stream is a MutationDataChunk
 */
object UpdaterPipeline {
  def build(opts: UpdaterPipelineOptions, incomingStreams: List[DataStream[InputEvent]],
            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
            uniqueIdGenerator: () => String = UUID.randomUUID().toString,
            clock: Clock = Clock.systemUTC(),
            outputStreamName: String = "wdqs_streaming_updater")
           (implicit env: StreamExecutionEnvironment): UpdaterPipeline = {
    env.getConfig.registerTypeWithKryoSerializer(classOf[Patch], classOf[RDFPatchSerializer])
    val incomingEventStream: DataStream[InputEvent] = incomingStreams match {
      case Nil => throw new NoSuchElementException("at least one stream is needed")
      case x :: Nil => x
      case x :: rest => x.union(rest: _*)
    }
    val windowStream: DataStream[InputEvent] = EventReorderingWindowFunction.attach(incomingEventStream,
      Time.milliseconds(opts.reorderingWindowLengthMs), opts.reorderingOpParallelism)
    val lateEventsSideOutput: DataStream[InputEvent] = windowStream.getSideOutput(EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG)

    val outputMutationStream: DataStream[MutationOperation] = rerouteIgnoredMutations(decideMutationOp(windowStream, opts))
    val spuriousEventsLate: DataStream[IgnoredMutation] = outputMutationStream.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS)

    val resolvedOpStream: DataStream[ResolvedOp] = resolveMutationOperations(opts, wikibaseRepositoryGenerator, outputMutationStream)
    val patchStream: DataStream[SuccessfulOp] = rerouteFailedOps(resolvedOpStream, opts)
    val failedOpsToSideOutput: DataStream[FailedOp] = patchStream.getSideOutput(RouteFailedOpsToSideOutput.FAILED_OPS_TAG)

    val tripleStream: DataStream[MutationDataChunk] = measureLatency(
      rdfPatchChunkOp(patchStream, opts, uniqueIdGenerator, clock, outputStreamName), clock, opts)

    new UpdaterPipeline(lateEventsSideOutput, spuriousEventsLate, failedOpsToSideOutput, tripleStream, updaterPipelineOptions = opts)
  }

  private def rerouteFailedOps(resolvedOpStream: DataStream[ResolvedOp], opts: UpdaterPipelineOptions): DataStream[SuccessfulOp] = {
    resolvedOpStream
      .process(new RouteFailedOpsToSideOutput())
      .name("RouteFailedOpsToSideOutput")
      .uid("RouteFailedOpsToSideOutput")
      .setParallelism(opts.outputParallelism)
  }

  private def rdfPatchChunkOp(dataStream: DataStream[SuccessfulOp],
                              opts: UpdaterPipelineOptions,
                              uniqueIdGenerator: () => String,
                              clock: Clock,
                              outputStreamName: String
                             ): DataStream[MutationDataChunk] = {
    dataStream
      .flatMap(new PatchChunkOperation(
        domain = opts.hostname,
        clock = clock,
        uniqueIdGenerator = uniqueIdGenerator,
        stream = outputStreamName
      ))
      .name("RDFPatchChunkOperation")
      .uid("RDFPatchChunkOperation")
      .setParallelism(opts.outputParallelism)
  }

  private def measureLatency(dataStream: DataStream[MutationDataChunk],
                             clock: Clock,
                             updaterPipelineOptions: UpdaterPipelineOptions
                            ): DataStream[MutationDataChunk] = {
    dataStream.map(MeasureEventProcessingLatencyOperation(clock))
      .name("MeasureEventProcessingLatencyOperation")
      .uid("MeasureEventProcessingLatencyOperation")
      .setParallelism(updaterPipelineOptions.outputParallelism)
  }

  private def resolveMutationOperations(opts: UpdaterPipelineOptions,
                                        wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                        outputMutationStream: DataStream[MutationOperation]
                                       ): DataStream[ResolvedOp] = {
    val streamToResolve: KeyedStream[MutationOperation, String] = outputMutationStream.keyBy(_.item)

    val genDiffOperator: AsyncFunction[MutationOperation, ResolvedOp] = GenerateEntityDiffPatchOperation(
      domain = opts.hostname,
      wikibaseRepositoryGenerator = wikibaseRepositoryGenerator,
      poolSize = opts.wikibaseRepoThreadPoolSize
    )

    // poolSize * 2 for the number of inflight items is a random guess
    AsyncDataStream.orderedWait(streamToResolve, genDiffOperator, opts.generateDiffTimeout, MILLISECONDS,  opts.wikibaseRepoThreadPoolSize * 2)
      .name("GenerateEntityDiffPatchOperation")
      .uid("GenerateEntityDiffPatchOperation")
      .setParallelism(opts.generateDiffParallelism)
  }

  private def rerouteIgnoredMutations(allOutputMutationStream: DataStream[AllMutationOperation]): DataStream[MutationOperation] = {
    val outputMutationStream: DataStream[MutationOperation] = allOutputMutationStream
      .process(new RouteIgnoredMutationToSideOutput())
      .name("RouteIgnoredMutationToSideOutput")
      .uid("RouteIgnoredMutationToSideOutput")
    outputMutationStream
  }

  private def decideMutationOp(windowStream: DataStream[InputEvent], opts: UpdaterPipelineOptions): DataStream[AllMutationOperation] = {
    val allOutputMutationStream: DataStream[AllMutationOperation] = windowStream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .name("DecideMutationOperation")
      .uid("DecideMutationOperation")
    opts.decideMutationOpParallelism.foreach(allOutputMutationStream.setParallelism)
    allOutputMutationStream
  }
}
