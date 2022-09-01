package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant}
import java.util
import java.util.{Timer, TimerTask}
import java.util.Collections.emptyList
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.Collector
import org.openrdf.model.Statement
import org.wikidata.query.rdf.common.uri.UrisScheme
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, Patch}
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND
import org.wikidata.query.rdf.updater.GenerateEntityDiffPatchOperation.mungerOperationProvider

sealed trait ResolvedOp {
  val operation: MutationOperation
}

abstract class SuccessfulOp extends ResolvedOp

case class EntityPatchOp(operation: MutationOperation,
                         data: Patch) extends SuccessfulOp
case class ReconcileOp(operation: Reconcile,
                         data: Patch) extends SuccessfulOp
case class FailedOp(operation: MutationOperation, exception: ContainedException) extends ResolvedOp

case class DeleteOp(override val operation: MutationOperation) extends SuccessfulOp

case class GenerateEntityDiffPatchOperation(
                                             scheme: UrisScheme,
                                             wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                             mungeOperationProvider: UrisScheme => (String, util.Collection[Statement]) => Long = mungerOperationProvider,
                                             acceptableRepositoryLag: FiniteDuration,
                                             poolSize: Int = 10,
                                             fetchAttempts: Int = 4,
                                             fetchRetryDelay: FiniteDuration = 1.seconds,
                                             now: () => Instant = () => Clock.systemUTC.instant
                                           )
  extends RichAsyncFunction[MutationOperation, ResolvedOp] {

  lazy val repository: WikibaseEntityRevRepositoryTrait =  wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val diff: EntityDiff = EntityDiff.withWikibaseSharedElements(scheme)

  implicit lazy val executionContext: ExecutionContext = buildExecContext

  private def buildExecContext = {
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(poolSize,
      new ThreadFactoryBuilder().setNameFormat("GenerateEntityDiffPatchOperation-fetcher-%d-" + getRuntimeContext.getIndexOfThisSubtask).build()))
  }

  lazy val mungeOperation: (String, util.Collection[Statement]) => Long = mungeOperationProvider.apply(scheme)

  override def asyncInvoke(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    if (op.isInstanceOf[DeleteItem]) {
      resultFuture.complete(DeleteOp(op) :: Nil)
    } else {
      completeFetchContent(op, resultFuture)
    }
  }

  private def recentEventCutoff = now().minusMillis(acceptableRepositoryLag.toMillis)
  private def isRecent(eventTime: Instant): Boolean = eventTime.isAfter(recentEventCutoff)

  private def completeFetchContent(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    val future: Future[Patch] = op match {
      case Diff(item, eventTime, revision, fromRev, _, _) =>
        getDiff(item, revision, fromRev, eventTime)
      case FullImport(item, eventTime, revision, _, _) =>
        getImport(item, revision, eventTime)
      case Reconcile(item, _, revision, _, _) =>
        getReconcile(item, revision)
      case DeleteItem(item, _, _, _, _) => throw new IllegalArgumentException(
        s"Received content fetch for DeleteItem($item, ...)")
    }

    future.onComplete (patch => {
      (patch, op) match {
        case (Success(patch: Patch), r: Reconcile) => resultFuture.complete(ReconcileOp(r, patch) :: Nil)
        case (Success(patch), _) => resultFuture.complete(EntityPatchOp(op, patch) :: Nil)
        case (Failure(exception: ContainedException), _) => resultFuture.complete(FailedOp(op, exception) :: Nil)
        case (Failure(exception: Throwable), _) => resultFuture.completeExceptionally(exception)
      }
    })
  }

  private def isRetryable(eventTime: Option[Instant], attempts: Int, t: Throwable): Boolean = t match {
    case _: RetryableException => attempts < fetchAttempts
    case e: WikibaseEntityFetchException => e.getErrorType == ENTITY_NOT_FOUND && eventTime.exists(isRecent)
    case _: Throwable => false
  }

  private def fetchAsync(item: String, revision: Long, eventTime: Option[Instant] = None): Future[Iterable[Statement]] = {
    RetryingFuture(fetchRetryDelay, isRetryable(eventTime, _, _)) {
      repository.getEntityByRevision(item, revision)
    } recoverWith {
      case e: FailedRetryException => e.getCause match {
        case cause: RetryableException => Future.failed(
          new ContainedException(s"Cannot fetch entity $item revision $revision failed ${e.attempts} times, abandoning.", cause))
        case cause: Throwable => Future.failed(cause)
      }
    }
  }

  private def getDiff(item: String, revision: Long, fromRev: Long, eventTime: Instant): Future[Patch] = {
    val from: Future[Iterable[Statement]] = fetchAsync(item, fromRev)
    val to: Future[Iterable[Statement]] = fetchAsync(item, revision, Some(eventTime))

    from flatMap { fromIt =>
      to map {
        toIt => generateDiff(item, fromIt, toIt, revision, fromRev)
      }
    }
  }

  private def getImport(item: String, revision: Long, eventTime: Instant): Future[Patch] = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(item, revision, Some(eventTime))
    stmts map { sendImport(item, _, revision) }
  }

  private def getReconcile(item: String, revision: Long): Future[Patch] = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(item, revision)
    stmts map { sendReconcile(item, _, revision) }
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

  private def sendReconcile(item: String, statements: Iterable[Statement], revision: Long): Patch = {
    if (statements.isEmpty) {
      throw new ContainedException(s"Got empty entity for $item, revision:$revision (404 or 204?)")
    }
    val toList = new util.ArrayList[Statement](statements.asJavaCollection)
    mungeOperation(item, toList)
    new Patch(toList, emptyList(), emptyList(), emptyList())
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

  object RetryingFuture {
    private val timer = new Timer(true)

    private def after[T](delay: FiniteDuration)(op: => Future[T]): Future[T] = {
      val promise: Promise[T] = Promise()
      timer.schedule(new TimerTask {
        override def run(): Unit = promise completeWith op
      }, delay.toMillis)
      promise.future
    }

    def apply[T](delay: FiniteDuration, canRetry: (Int, Throwable) => Boolean)(op: => T): Future[T] = {
      def attempt(attempts: Int): Future[T] =
        Future(op) recoverWith {
          case e: Throwable if canRetry(attempts, e) => after(delay)(attempt(attempts + 1))
          case e: Throwable => Future.failed(new FailedRetryException(attempts, e))
        }
      attempt(1)
    }
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

class FailedRetryException(val attempts: Int, cause: Throwable) extends RuntimeException(cause)

