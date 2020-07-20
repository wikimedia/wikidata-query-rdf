package org.wikidata.query.rdf.updater

import java.time.Clock
import java.util
import java.util.function.Supplier
import java.util.Collections.emptyList
import java.util.UUID
import java.util.concurrent.{Executors, ThreadFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector
import org.openrdf.model.Statement
import org.openrdf.rio.{RDFFormat, RDFWriterRegistry}
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.{UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, RDFPatch}
import org.wikidata.query.rdf.tool.stream.{MutationEventData, MutationEventDataGenerator, RDFChunkSerializer}
import org.wikidata.query.rdf.updater.GenerateEntityDiffPatchOperation.mungerOperationProvider

sealed trait ResolvedOp {
  val operation: MutationOperation
}

case class EntityPatchOp(operation: MutationOperation,
                         data: MutationEventData) extends ResolvedOp
case class FailedOp(operation: MutationOperation, error: String) extends ResolvedOp

case class GenerateEntityDiffPatchOperation(domain: String,
                                            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                            mimeType: String = RDFFormat.TURTLE.getDefaultMIMEType,
                                            chunkSoftMaxSize: Int = 128000, // ~max 128k chars, can be slightly more
                                            clock: Clock = Clock.systemUTC(),
                                            uniqueIdGenerator: () => String = () => UUID.randomUUID().toString,
                                            stream: String = "wdqs_streaming_updater",
                                            mungeOperationProvider: UrisScheme => (String, util.Collection[Statement]) => Long = s => mungerOperationProvider(s)
                                  )
  extends RichFlatMapFunction[MutationOperation, ResolvedOp] {


  private val LOG = LoggerFactory.getLogger(getClass)

  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val scheme: UrisScheme = UrisSchemeFactory.forHost(domain)
  lazy val diff: EntityDiff = EntityDiff.withValuesAndRefsAsSharedElements(scheme)
  lazy val rdfSerializer: RDFChunkSerializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance())
  lazy val dataEventGenerator: MutationEventDataGenerator = new MutationEventDataGenerator(rdfSerializer, mimeType, chunkSoftMaxSize)

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10,
    new ThreadFactoryBuilder().setNameFormat("GenerateEntityDiffPatchOperation-fetcher-%d").build()))
  lazy val mungeOperation: (String, util.Collection[Statement]) => Long = mungeOperationProvider.apply(scheme)


  private def fetchAsync(item: String, revision: Long): Future[Iterable[Statement]] = {
    @scala.annotation.tailrec
    def retryableFetch(attempt: Int = 1, maxAttempt: Int = 4): Iterable[Statement] = {
      try {
        repository.getEntityByRevision(item, revision)
      } catch {
        case exception: RetryableException =>
          if (attempt >= maxAttempt) {
            throw new ContainedException(s"Cannot fetch entity $item revision $revision failed $maxAttempt times, abandoning.")
          } else {
            LOG.warn(s"Exception thrown fetching $item, $revision, retrying ($attempt/$maxAttempt): ${exception.getMessage}.")
            retryableFetch(attempt + 1, maxAttempt)
          }
      }
    }
    Future { retryableFetch() }
  }

  private def fetchTwoAndSendDiff(op: MutationOperation, item: String, revision: Long, fromRev: Long, origMeta: EventsMeta,
                                  collector: Collector[ResolvedOp]): Unit = {
    val from: Future[Iterable[Statement]] = fetchAsync(item, fromRev)
    val to: Future[Iterable[Statement]] = fetchAsync(item, revision)

    val awaiting: Future[Unit] = from flatMap { fromIt =>
      to map {
        toIt => generateAndSendDiff(op, item, fromIt, toIt, revision, fromRev, origMeta, collector)
      }
    }

    Await.result(awaiting, Duration.Inf)
  }

  private def fetchOneAndSendImport(op: MutationOperation, item: String, revision: Long, origMeta: EventsMeta, collector: Collector[ResolvedOp]): Unit = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(item, revision)
    val awaiting: Future[Unit] = stmts map {
      sendImport(op, item, _, revision, origMeta, collector)
    }

    Await.result(awaiting, Duration.Inf)
  }

  override def flatMap(op: MutationOperation, collector: Collector[ResolvedOp]): Unit = {
    try {
      op match {
        case Diff(item, _, revision, fromRev, _, origMeta) =>
          fetchTwoAndSendDiff(op, item, revision, fromRev, origMeta, collector)
        case FullImport(item, _, revision, _, origMeta) =>
          fetchOneAndSendImport(op, item, revision, origMeta, collector)
      }
    } catch {
      case e: ContainedException => collector.collect(FailedOp(op, e.toString))
    }
  }

  private def sendImport(op: MutationOperation, item: String, stmts: Iterable[Statement], revision: Long, origMeta: EventsMeta,
                         collector: Collector[ResolvedOp]): Unit = {
      if (stmts.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, revision:$revision (404 or 204?)")
      } else {
        val patch = fullImport(item, stmts)
        dataEventGenerator.fullImportEvent(eventMetaSupplier(origMeta), item, revision, op.eventTime,
            patch.getAdded, patch.getLinkedSharedElements).asScala
          .map(EntityPatchOp(op, _))
          .foreach(collector.collect(_))
      }
  }

  private def generateAndSendDiff(op: MutationOperation, item: String, from: Iterable[Statement], to: Iterable[Statement],
                                  revision: Long, fromRev: Long, origMeta: EventsMeta, collector: Collector[ResolvedOp]): Unit = {
      if (from.isEmpty || to.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, fromRev: $fromRev, revision:$revision (404 or 204?)")
      } else {
        val patch = diff(item, from, to)
        dataEventGenerator.diffEvent(eventMetaSupplier(origMeta), item, revision, op.eventTime,
            patch.getAdded, patch.getRemoved, patch.getLinkedSharedElements, patch.getUnlinkedSharedElements).asScala
          .map(EntityPatchOp(op, _))
          .foreach(collector.collect(_))
    }
  }

  private def eventMetaSupplier(originalEventMeta: EventsMeta): Supplier[EventsMeta] = {
      new Supplier[EventsMeta] {
        override def get(): EventsMeta = new EventsMeta(clock.instant(), uniqueIdGenerator.apply(), domain, stream, originalEventMeta.requestId())
      }
  }

  private def diff(item: String, from: Iterable[Statement], to: Iterable[Statement]): RDFPatch = {
    val fromList = new util.ArrayList[Statement](from.asJavaCollection)
    val toList = new util.ArrayList[Statement](to.asJavaCollection)

    mungeOperation(item, fromList)
    mungeOperation(item, toList)

    diff.diff(fromList, toList)
  }

  private def fullImport(item: String, stmts: Iterable[Statement]): RDFPatch = {
    val toList = new util.ArrayList[Statement](stmts.asJavaCollection)

    mungeOperation(item, toList)

    diff.diff(emptyList(), toList)
  }
}

object GenerateEntityDiffPatchOperation {
  private def mungerOperationProvider(scheme: UrisScheme): (String, util.Collection[Statement]) => Long = {
    val munger = Munger.builder(scheme).convertBNodesToSkolemIRIs(true).build()
    (item, lst) => munger.munge(item, lst)
  }
}

sealed class RouteFailedOpsToSideOutput(ignoredEventTag: OutputTag[FailedOp] = RouteFailedOpsToSideOutput.FAILED_OPS_TAG)
  extends ProcessFunction[ResolvedOp, EntityPatchOp]
{
  override def processElement(i: ResolvedOp,
                              context: ProcessFunction[ResolvedOp, EntityPatchOp]#Context,
                              collector: Collector[EntityPatchOp]
                             ): Unit = {
    i match {
      case e: FailedOp => context.output(ignoredEventTag, e)
      case x: EntityPatchOp => collector.collect(x)
    }
  }
}

object RouteFailedOpsToSideOutput {
  val FAILED_OPS_TAG: OutputTag[FailedOp] = new OutputTag[FailedOp]("failed-ops-events")
}

