package org.wikidata.query.rdf.updater

import java.util
import java.util.Collections.emptyList
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector
import org.openrdf.model.Statement
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.{UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, RDFPatch}
import org.wikidata.query.rdf.updater.GenerateEntityDiffPatchOperation.mungerOperationProvider

sealed trait ResolvedOp {
  val operation: MutationOperation
}

case class EntityPatchOp(operation: MutationOperation,
                         data: RDFPatch) extends ResolvedOp
case class FailedOp(operation: MutationOperation, error: String) extends ResolvedOp

case class GenerateEntityDiffPatchOperation(domain: String,
                                            wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                            mungeOperationProvider: UrisScheme => (String, util.Collection[Statement]) => Long = s => mungerOperationProvider(s)
                                  )
  extends RichMapFunction[MutationOperation, ResolvedOp] {


  private val LOG = LoggerFactory.getLogger(getClass)

  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val scheme: UrisScheme = UrisSchemeFactory.forHost(domain)
  lazy val diff: EntityDiff = EntityDiff.withValuesAndRefsAsSharedElements(scheme)

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10,
    new ThreadFactoryBuilder().setNameFormat("GenerateEntityDiffPatchOperation-fetcher-%d").build()))
  lazy val mungeOperation: (String, util.Collection[Statement]) => Long = mungeOperationProvider.apply(scheme)

  override def map(op: MutationOperation): ResolvedOp = {
    try {
      op match {
        case Diff(item, _, revision, fromRev, _, _) =>
          getDiff(op, item, revision, fromRev)
        case FullImport(item, _, revision, _, _) =>
          getImport(op, item, revision)
      }
    } catch {
      case e: ContainedException => FailedOp(op, e.toString)
    }
  }

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

  private def getDiff(op: MutationOperation, item: String, revision: Long, fromRev: Long): ResolvedOp = {
    val from: Future[Iterable[Statement]] = fetchAsync(item, fromRev)
    val to: Future[Iterable[Statement]] = fetchAsync(item, revision)

    val awaiting: Future[RDFPatch] = from flatMap { fromIt =>
      to map {
        toIt => generateDiff(item, fromIt, toIt, revision, fromRev)
      }
    }

    EntityPatchOp(op, Await.result(awaiting, Duration.Inf))
  }

  private def getImport(op: MutationOperation, item: String, revision: Long): ResolvedOp = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(item, revision)
    val awaiting: Future[RDFPatch] = stmts map {
      sendImport(item, _, revision)
    }

    EntityPatchOp(op, Await.result(awaiting, Duration.Inf))
  }

  private def sendImport(item: String, stmts: Iterable[Statement], revision: Long): RDFPatch = {
      if (stmts.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, revision:$revision (404 or 204?)")
      } else {
        fullImport(item, stmts)
      }
  }

  private def generateDiff(item: String, from: Iterable[Statement], to: Iterable[Statement],
                           revision: Long, fromRev: Long): RDFPatch = {
      if (from.isEmpty || to.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, fromRev: $fromRev, revision:$revision (404 or 204?)")
      } else {
        diff(item, from, to)
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

