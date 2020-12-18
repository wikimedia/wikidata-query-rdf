package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util.UUID

import scala.concurrent.duration.MILLISECONDS

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.{DiscardingSink, SinkFunction}
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.wikidata.query.rdf.tool.rdf.Patch
import org.wikidata.query.rdf.updater.config.UpdaterPipelineGeneralConfig
import org.wikidata.query.rdf.updater.UpdaterPipeline.OUTPUT_PARALLELISM

sealed class UpdaterPipeline(lateEventStream: DataStream[InputEvent],
                             spuriousEventStream: DataStream[IgnoredMutation],
                             failedOpsStream: DataStream[FailedOp],
                             tripleEventStream: DataStream[MutationDataChunk],
                             updaterPipelineOptions: UpdaterPipelineGeneralConfig
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
      .uid("failed-events-output")
      .name("failed-events-output")
    this
  }

  def saveMutationsTo(sink: SinkFunction[MutationDataChunk]): UpdaterPipeline = {
    tripleEventStream.addSink(sink)
      .uid(updaterPipelineOptions.outputOperatorNameAndUuid)
      .name(updaterPipelineOptions.outputOperatorNameAndUuid)
      .setParallelism(OUTPUT_PARALLELISM)
    this
  }

  def saveTo(sinks: (SinkFunction[MutationDataChunk], SinkFunction[InputEvent], SinkFunction[IgnoredMutation], SinkFunction[FailedOp])): UpdaterPipeline = {
    saveMutationsTo(sinks._1)
      .saveLateEventsTo(sinks._2)
      .saveSpuriousEventsTo(sinks._3)
      .saveFailedOpsTo(sinks._4)
  }
}

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
  // Enforce output parallelism of 1 ( to ensure proper ordering of the output patches
  private val OUTPUT_PARALLELISM = 1

  def build(opts: UpdaterPipelineGeneralConfig, incomingStreams: List[DataStream[InputEvent]],
            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
            uniqueIdGenerator: () => String = UUID.randomUUID().toString,
            clock: Clock = Clock.systemUTC(),
            outputStreamName: String = "wdqs_streaming_updater")
           (implicit env: StreamExecutionEnvironment): UpdaterPipeline = {
    val incomingEventStream: KeyedStream[InputEvent, String] =
      (incomingStreams match {
        case Nil => throw new NoSuchElementException("at least one stream is needed")
        case x :: Nil => x
        case x :: rest => x.union(rest: _*)
      }).keyBy(_.item)


    env.getConfig.registerTypeWithKryoSerializer(classOf[Patch], classOf[RDFPatchSerializer])
    val (outputMutationStream, lateEventsSideOutput, spuriousEventsLate):
      (DataStream[MutationOperation], DataStream[InputEvent], DataStream[IgnoredMutation]) = {
      val stream = ReorderAndDecideMutationOperation.attach(incomingEventStream, opts.reorderingWindowLengthMs)
      (stream, stream.getSideOutput(ReorderAndDecideMutationOperation.LATE_EVENTS_SIDE_OUTPUT_TAG),
        stream.getSideOutput(ReorderAndDecideMutationOperation.SPURIOUS_REV_EVENTS))
    }

    val resolvedOpStream: DataStream[ResolvedOp] = resolveMutationOperations(opts, wikibaseRepositoryGenerator, outputMutationStream)
    val patchStream: DataStream[SuccessfulOp] = rerouteFailedOps(resolvedOpStream)
    val failedOpsToSideOutput: DataStream[FailedOp] = patchStream.getSideOutput(RouteFailedOpsToSideOutput.FAILED_OPS_TAG)

    val tripleStream: DataStream[MutationDataChunk] = measureLatency(
      rdfPatchChunkOp(patchStream, opts, uniqueIdGenerator, clock, outputStreamName), clock)

    new UpdaterPipeline(lateEventsSideOutput, spuriousEventsLate, failedOpsToSideOutput, tripleStream, updaterPipelineOptions = opts)
  }

  private def rerouteFailedOps(resolvedOpStream: DataStream[ResolvedOp]): DataStream[SuccessfulOp] = {
    resolvedOpStream
      .process(new RouteFailedOpsToSideOutput())
      .name("RouteFailedOpsToSideOutput")
      .uid("RouteFailedOpsToSideOutput")
  }

  private def rdfPatchChunkOp(dataStream: DataStream[SuccessfulOp],
                              opts: UpdaterPipelineGeneralConfig,
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
      .setParallelism(OUTPUT_PARALLELISM)
  }



  private def measureLatency(dataStream: DataStream[MutationDataChunk],
                             clock: Clock): DataStream[MutationDataChunk] = {
    dataStream.map(MeasureEventProcessingLatencyOperation(clock))
      .name("MeasureEventProcessingLatencyOperation")
      .uid("MeasureEventProcessingLatencyOperation")
      .setParallelism(OUTPUT_PARALLELISM)
  }

  private def resolveMutationOperations(opts: UpdaterPipelineGeneralConfig,
                                        wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                        outputMutationStream: DataStream[MutationOperation]
                                       ): DataStream[ResolvedOp] = {
    val streamToResolve: KeyedStream[MutationOperation, String] = new DataStreamUtils(outputMutationStream).reinterpretAsKeyedStream(_.item)

    val genDiffOperator: AsyncFunction[MutationOperation, ResolvedOp] = GenerateEntityDiffPatchOperation(
      domain = opts.hostname,
      wikibaseRepositoryGenerator = wikibaseRepositoryGenerator,
      poolSize = opts.wikibaseRepoThreadPoolSize
    )

    // poolSize * 2 for the number of inflight items is a random guess
    AsyncDataStream.orderedWait(streamToResolve, genDiffOperator, opts.generateDiffTimeout, MILLISECONDS,  opts.wikibaseRepoThreadPoolSize * 2)
      .name("GenerateEntityDiffPatchOperation")
      .uid("GenerateEntityDiffPatchOperation")
  }
}
