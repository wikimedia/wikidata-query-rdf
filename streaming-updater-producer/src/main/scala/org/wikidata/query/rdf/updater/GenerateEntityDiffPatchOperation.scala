package org.wikidata.query.rdf.updater

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector
import org.openrdf.model.{Statement, URI}
import org.wikidata.query.rdf.common.uri.UrisScheme
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Munger, Patch}
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND
import org.wikidata.query.rdf.updater.GenerateEntityDiffPatchOperation.mungerOperationProvider

import java.time.{Clock, Instant}
import java.util
import java.util.concurrent.Executors
import java.util.{Timer, TimerTask}
import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

sealed trait ResolvedOp {
  val operation: MutationOperation
}

abstract class SuccessfulOp extends ResolvedOp {
  val subgraph: Option[URI]
}

case class EntityPatchOp(operation: MutationOperation,
                         data: Patch, subgraph: Option[URI] = None) extends SuccessfulOp

case class ReconcileOp(operation: Reconcile,
                       data: Patch, subgraph: Option[URI] = None) extends SuccessfulOp

case class DeleteOp(operation: MutationOperation, subgraph: Option[URI] = None) extends SuccessfulOp

case class FailedOp(operation: MutationOperation, exception: ContainedException) extends ResolvedOp

case class GenerateEntityDiffPatchOperation(
                                             scheme: UrisScheme,
                                             wikibaseRepositoryGenerator: RuntimeContext => WikibaseEntityRevRepositoryTrait,
                                             mungeOperationProvider: UrisScheme => (String, util.Collection[Statement]) => Long = mungerOperationProvider,
                                             acceptableRepositoryLag: FiniteDuration,
                                             poolSize: Int = 10,
                                             fetchAttempts: Int = 4,
                                             fetchRetryDelay: FiniteDuration = 1.seconds,
                                             now: () => Instant = () => Clock.systemUTC.instant,
                                             subgraphAssigner: SubgraphAssigner
                                           )
  extends RichAsyncFunction[MutationOperation, ResolvedOp] {

  lazy val repository: WikibaseEntityRevRepositoryTrait = wikibaseRepositoryGenerator(this.getRuntimeContext)
  lazy val entityDiff: EntityDiff = EntityDiff.withWikibaseSharedElements(scheme)
  lazy val subgraphDiff: SubgraphDiff = new SubgraphDiff(entityDiff, subgraphAssigner, scheme)

  type SubgraphPatch = (Option[URI], Patch)

  implicit lazy val executionContext: ExecutionContext = buildExecContext

  private def buildExecContext = {
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(poolSize,
      new ThreadFactoryBuilder().setNameFormat("GenerateEntityDiffPatchOperation-fetcher-%d-" + getRuntimeContext.getIndexOfThisSubtask).build()))
  }

  lazy val mungeOperation: (String, util.Collection[Statement]) => Long = mungeOperationProvider.apply(scheme)

  override def asyncInvoke(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    op match {
      case delete: DeleteItem =>
        resultFuture.complete(subgraphDiff.delete(delete))
      case _ =>
        completeFetchContent(op, resultFuture)
    }
  }

  private def recentEventCutoff = now().minusMillis(acceptableRepositoryLag.toMillis)

  private def isRecent(eventTime: Instant): Boolean = eventTime.isAfter(recentEventCutoff)

  // scalastyle:off cyclomatic.complexity
  private def completeFetchContent(op: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = {
    val future = op match {
      case Diff(_, eventTime, revision, fromRev, _, _) =>
        getDiff(op.asInstanceOf[Diff], revision, fromRev, eventTime)
      case FullImport(_, eventTime, revision, _, _) =>
        getImport(op.asInstanceOf[FullImport], revision, eventTime)
      case Reconcile(_, _, revision, _, _) =>
        getReconcile(op.asInstanceOf[Reconcile], revision)
      case DeleteItem(item, _, _, _, _) => throw new IllegalArgumentException(
        s"Received content fetch for DeleteItem($item, ...)")
    }

    future.onComplete {
      case Success(patches: Iterable[SuccessfulOp]) =>
        resultFuture.complete(patches)
      case Failure(exception: ContainedException) =>
        resultFuture.complete(FailedOp(op, exception) :: Nil)
      case Failure(exception: Throwable) =>
        resultFuture.completeExceptionally(exception)
    }
  }
  // scalastyle:on cyclomatic.complexity

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

  private def getDiff(operation: Diff, revision: Long, fromRev: Long, eventTime: Instant): Future[Iterable[SuccessfulOp]] = {
    val from: Future[Iterable[Statement]] = fetchAsync(operation.item, fromRev)
    val to: Future[Iterable[Statement]] = fetchAsync(operation.item, revision, Some(eventTime))

    from flatMap { fromIt =>
      to map {
        toIt => generateDiff(operation, fromIt, toIt, revision, fromRev)
      }
    }
  }

  private def getImport(operation: FullImport, revision: Long, eventTime: Instant): Future[Iterable[SuccessfulOp]] = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(operation.item, revision, Some(eventTime))
    stmts map {
      sendImport(operation, _, revision)
    }
  }

  private def getReconcile(operation: Reconcile, revision: Long): Future[Iterable[SuccessfulOp]] = {
    val stmts: Future[Iterable[Statement]] = fetchAsync(operation.item, revision)
    stmts map {
      sendReconcile(operation, _, revision)
    }
  }

  private def sendImport(operation: FullImport, stmts: Iterable[Statement], revision: Long): Iterable[SuccessfulOp] = {
    if (stmts.isEmpty) {
      throw new ContainedException(s"Got empty entity for ${operation.item}, revision:$revision (404 or 204?)")
    } else {
      fullImport(operation, stmts)
    }
  }

  private def generateDiff(operation: Diff, from: Iterable[Statement], to: Iterable[Statement],
                           revision: Long, fromRev: Long): Iterable[SuccessfulOp] = {
    if (from.isEmpty || to.isEmpty) {
      throw new ContainedException(s"Got empty entity for ${operation.item}, fromRev: $fromRev, revision:$revision (404 or 204?)")
    } else {
      diff(operation, from, to)
    }
  }

  private def sendReconcile(operation: Reconcile, statements: Iterable[Statement], revision: Long): Iterable[SuccessfulOp] = {
    if (statements.isEmpty) {
      throw new ContainedException(s"Got empty entity for $operation, revision:$revision (404 or 204?)")
    }
    val toList = new util.ArrayList[Statement](statements.asJavaCollection)
    mungeOperation(operation.item, toList)

    subgraphDiff.reconcile(operation, toList.asScala)
  }

  private def diff(operation: Diff, from: Iterable[Statement], to: Iterable[Statement]): Iterable[SuccessfulOp] = {
    val fromList = new util.ArrayList[Statement](from.asJavaCollection)
    val toList = new util.ArrayList[Statement](to.asJavaCollection)

    mungeOperation(operation.item, fromList)
    mungeOperation(operation.item, toList)

    subgraphDiff.diff(operation, fromList.asScala, toList.asScala)
  }

  private def fullImport(operation: FullImport, stmts: Iterable[Statement]): Iterable[SuccessfulOp] = {
    val toList = new util.ArrayList[Statement](stmts.asJavaCollection)

    mungeOperation(operation.item, toList)

    subgraphDiff.fullImport(operation, toList.asScala)
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
  extends ProcessFunction[ResolvedOp, SuccessfulOp] {
  override def processElement(op: ResolvedOp,
                              context: ProcessFunction[ResolvedOp, SuccessfulOp]#Context,
                              collector: Collector[SuccessfulOp]
                             ): Unit = {
    op match {
      case e: FailedOp => context.output(ignoredEventTag, e)
      case x: SuccessfulOp => collector.collect(x)
    }
  }
}

object RouteFailedOpsToSideOutput {
  val FAILED_OPS_TAG: OutputTag[FailedOp] = new OutputTag[FailedOp]("failed-ops-events")
}

class FailedRetryException(val attempts: Int, cause: Throwable) extends RuntimeException(cause)

