package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.AsyncFunction
import org.openrdf.model.URI
import org.wikidata.query.rdf.tool.rdf.Patch
import org.wikidata.query.rdf.updater.config.UpdaterPipelineGeneralConfig

import java.time.Clock
import java.util.UUID
import scala.concurrent.duration.MILLISECONDS

/**
 * Current state
 * stream1 = kafka(with periodic watermarks)
 * => filter(domain == "something")
 * => map(event convertion)
 * stream2 = same principle
 *
 * union of all streams
 * => keyBy (used as a partitioner to reduce cardinality)
 * => process(partial reordering and decide mutation op) see ReorderAndDecideMutationOperation
 * |=> side output 1: late events
 * |=> side output 2: spurious events
 * => process(fetch data from wikibase and diff) see GenerateEntityDiffPatchOperation
 * |=> side output: failed ops
 * => flatMap(split large patches into chunks) see RDFPatchChunkOperation
 * => measure latency
 * output of the stream is a MutationDataChunk
 */
object UpdaterPipeline {
  // Enforce output parallelism of 1 ( to ensure proper ordering of the output patches
  private val OUTPUT_PARALLELISM = 1

  // scalastyle:off parameter.number
  def configure(opts: UpdaterPipelineGeneralConfig,
                incomingStreams: List[DataStream[InputEvent]],
                outputStreams: OutputStreams,
                wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                uniqueIdGenerator: () => String = () => UUID.randomUUID().toString,
                clock: Clock = Clock.systemUTC(),
                mainStream: String,
                subgraphAssigner: SubgraphAssigner
               )
               (implicit env: StreamExecutionEnvironment): Unit = {

    val incomingEventStream: KeyedStream[InputEvent, String] =
      (incomingStreams match {
        case Nil => throw new NoSuchElementException("at least one stream is needed")
        case x :: Nil => x
        case x :: rest => x.union(rest: _*)
      }).keyBy(_.item)

    if (env.getParallelism < OUTPUT_PARALLELISM) {
      throw new IllegalAccessException("The job parallelism cannot be less than than the OUTPUT_PARALLELISM")
    }

    env.getConfig.registerTypeWithKryoSerializer(classOf[Patch], classOf[RDFPatchSerializer])
    val (outputMutationStream, lateEventsSideOutput, spuriousEventsSideOutput):
      (DataStream[MutationOperation], DataStream[InputEvent], DataStream[InconsistentMutation]) = {
      val stream = ReorderAndDecideMutationOperation.attach(incomingEventStream, opts.reorderingWindowLengthMs)
      (stream, stream.getSideOutput(ReorderAndDecideMutationOperation.LATE_EVENTS_SIDE_OUTPUT_TAG),
        stream.getSideOutput(ReorderAndDecideMutationOperation.SPURIOUS_REV_EVENTS))
    }

    val subgraphUriToStream: Map[URI, String] = subgraphAssigner.distinctSubgraphs
      .map(subgraph => subgraph.getSubgraphUri -> subgraph.getStream).toMap

    val resolvedOpStream: DataStream[ResolvedOp] = resolveMutationOperations(opts, wikibaseRepositoryGenerator, outputMutationStream, subgraphAssigner)

    val successfulOpStream: DataStream[SuccessfulOp] = rerouteFailedOps(resolvedOpStream)
    val failedOpsToSideOutput: DataStream[FailedOp] = successfulOpStream.getSideOutput(RouteFailedOpsToSideOutput.FAILED_OPS_TAG)
    val patchStream = measureLatency(rdfPatchChunkOp(successfulOpStream, opts, uniqueIdGenerator, clock, mainStream, subgraphUriToStream), clock)
    attachSinks(outputStreams, lateEventsSideOutput, spuriousEventsSideOutput, failedOpsToSideOutput, patchStream)
  }
  // scalastyle:on parameter.number

  private def attachSinks(outputStreams: OutputStreams,
                          lateEventsSideOutput: DataStream[InputEvent],
                          spuriousEventsSideOutput: DataStream[InconsistentMutation],
                          failedOpsToSideOutput: DataStream[FailedOp],
                          patchStream: DataStream[MutationDataChunk]): Unit = {
    outputStreams.lateEventsSink foreach {
      _.attachStream(lateEventsSideOutput)
    }
    outputStreams.spuriousEventsSink foreach {
      _.attachStream(spuriousEventsSideOutput)
    }
    outputStreams.failedOpsSink foreach {
      _.attachStream(failedOpsToSideOutput)
    }
    outputStreams.mutationSink
      .attachStream(patchStream)
      .setParallelism(OUTPUT_PARALLELISM)
  }

  private def rerouteFailedOps(resolvedOpStream: DataStream[ResolvedOp]): DataStream[SuccessfulOp] = {
    resolvedOpStream
      .process(new RouteFailedOpsToSideOutput())
      .name("RouteFailedOpsToSideOutput")
      .uid("RouteFailedOpsToSideOutput")
      .setParallelism(OUTPUT_PARALLELISM)
  }

  private def rdfPatchChunkOp(dataStream: DataStream[SuccessfulOp],
                              opts: UpdaterPipelineGeneralConfig,
                              uniqueIdGenerator: () => String,
                              clock: Clock,
                              mainStream: String,
                              subgraphUriToStreamMap: Map[URI, String]
                             ): DataStream[MutationDataChunk] = {
    dataStream
      // explicitly rescale to avoid re-ordering events (OUTPUT_PARALLELISM must be smaller than the job parallelism),
      // should not matter much as long as OUTPUT_PARALLELISM is 1 but be explicit to expression the intention of keeping
      // the event order from their source partitions
      .rescale
      .flatMap(new PatchChunkOperation(
        domain = opts.hostname,
        clock = clock,
        uniqueIdGenerator = uniqueIdGenerator,
        mainStream = mainStream,
        subgraphStreams =  subgraphUriToStreamMap,
        mutationEventDataFactory = opts.outputMutationSchema match {
          case "v1" => MutationEventDataFactory.v1()
          case "v2" => MutationEventDataFactory.v2()
          case _ @ v => throw new IllegalArgumentException(s"Unsupported schema version $v")
        }
      ))
      .name(s"RDFPatchChunkOperation")
      .uid(s"RDFPatchChunkOperation")
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
                                        outputMutationStream: DataStream[MutationOperation],
                                        subgraphAssigner: SubgraphAssigner
                                       ): DataStream[ResolvedOp] = {
    val genDiffOperator: AsyncFunction[MutationOperation, ResolvedOp] = GenerateEntityDiffPatchOperation(
      scheme = opts.urisScheme,
      wikibaseRepositoryGenerator = wikibaseRepositoryGenerator,
      poolSize = opts.wikibaseRepoThreadPoolSize,
      acceptableRepositoryLag = opts.acceptableMediawikiLag,
      subgraphAssigner = subgraphAssigner
    )

    // poolSize * 2 for the number of inflight items is a random guess
    AsyncDataStream.orderedWait(outputMutationStream,
        genDiffOperator, opts.generateDiffTimeout, MILLISECONDS, opts.wikibaseRepoThreadPoolSize * 2)
      .name("GenerateEntityDiffPatchOperation")
      .uid("GenerateEntityDiffPatchOperation")
  }
}
