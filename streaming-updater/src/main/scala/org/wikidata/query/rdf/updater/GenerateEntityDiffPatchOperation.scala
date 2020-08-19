package org.wikidata.query.rdf.updater

import java.util
import java.util.Collections.emptyList
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

import scala.concurrent.{ExecutionContext, Future}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.Collector
import org.openrdf.model.Statement
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.{UrisScheme, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, Patch}
import org.wikidata.query.rdf.updater.GenerateEntityDiffPatchOperation.mungerOperationProvider

sealed trait ResolvedOp {
  val operation: MutationOperation
}

abstract class SuccessfulOp extends ResolvedOp

case class EntityPatchOp(operation: MutationOperation,
                         data: Patch) extends SuccessfulOp
case class FailedOp(operation: MutationOperation, error: String) extends ResolvedOp

case class DeleteOp(override val operation: MutationOperation) extends SuccessfulOp

case class GenerateEntityDiffPatchOperation(
                                             domain: String,
                                             wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                             mungeOperationProvider: UrisScheme => (String, util.Collection[Statement]) => Long = mungerOperationProvider,
                                             poolSize: Int = 10
                                           )
  extends RichAsyncFunction[MutationOperation, ResolvedOp] {

  private val LOG = LoggerFactory.getLogger(getClass)

  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val scheme: UrisScheme = UrisSchemeFactory.forHost(domain)
  lazy val diff: EntityDiff = EntityDiff.withValuesAndRefsAsSharedElements(scheme)

  implicit lazy val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(poolSize,
    new ThreadFactoryBuilder().setNameFormat("GenerateEntityDiffPatchOperation-fetcher-%d").build()))
  lazy val mungeOperation: (String, util.Collection[Statement]) => Long = mungeOperationProvider.apply(scheme)

  override def asyncInvoke(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    if (op.isInstanceOf[DeleteItem]) {
      resultFuture.complete(DeleteOp(op) :: Nil)
    } else {
      completeDiffPatch(op, resultFuture)
    }
  }

  private def completeDiffPatch(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    val future: Future[Patch] = op match {
      case Diff(item, _, revision, fromRev, _, _) =>
        getDiff(item, revision, fromRev)
      case FullImport(item, _, revision, _, _) =>
        getImport(item, revision)
    }
    future.onComplete {
      case Success(patch) => resultFuture.complete(EntityPatchOp(op, patch) :: Nil)
      case Failure(exception: ContainedException) => resultFuture.complete(FailedOp(op, exception.toString) :: Nil)
      case Failure(exception: Throwable) => resultFuture.completeExceptionally(exception)
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

  private def getDiff(item: String, revision: Long, fromRev: Long): Future[Patch] = {
    val from: Future[Iterable[Statement]] = fetchAsync(item, fromRev)
    val to: Future[Iterable[Statement]] = fetchAsync(item, revision)

    from flatMap { fromIt =>
      to map {
        toIt => generateDiff(item, fromIt, toIt, revision, fromRev)
      }
    }
  }

  private def getImport(item: String, revision: Long): Future[Patch] = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(item, revision)
    stmts map { sendImport(item, _, revision) }
  }

  private def sendImport(item: String, stmts: Iterable[Statement], revision: Long): Patch = {
      if (stmts.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, revision:$revision (404 or 204?)")
      } else {
        fullImport(item, stmts)
      }
  }

  private def generateDiff(item: String, from: Iterable[Statement], to: Iterable[Statement],
                           revision: Long, fromRev: Long): Patch = {
      if (from.isEmpty || to.isEmpty) {
        throw new ContainedException(s"Got empty entity for $item, fromRev: $fromRev, revision:$revision (404 or 204?)")
      } else {
        diff(item, from, to)
    }
  }

  private def diff(item: String, from: Iterable[Statement], to: Iterable[Statement]): Patch = {
    val fromList = new util.ArrayList[Statement](from.asJavaCollection)
    val toList = new util.ArrayList[Statement](to.asJavaCollection)

    mungeOperation(item, fromList)
    mungeOperation(item, toList)

    diff.diff(fromList, toList)
  }

  private def fullImport(item: String, stmts: Iterable[Statement]): Patch = {
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
  extends ProcessFunction[ResolvedOp, SuccessfulOp]
{
  override def processElement(i: ResolvedOp,
                              context: ProcessFunction[ResolvedOp, SuccessfulOp]#Context,
                              collector: Collector[SuccessfulOp]
                             ): Unit = {
    i match {
      case e: FailedOp => context.output(ignoredEventTag, e)
      case x: SuccessfulOp => collector.collect(x)
    }
  }
}

object RouteFailedOpsToSideOutput {
  val FAILED_OPS_TAG: OutputTag[FailedOp] = new OutputTag[FailedOp]("failed-ops-events")
}

