package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.Instant
import java.util.Collections.emptyList

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.operators.testutils.MockEnvironment
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory
import org.apache.flink.streaming.api.scala.async
import org.apache.flink.streaming.util.{MockStreamingRuntimeContext, OneInputStreamOperatorTestHarness}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.test.StatementHelper.statements
import org.wikidata.query.rdf.tool.change.events.EventInfo
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.Patch
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException

class GenerateEntityDiffPatchOperationUnitTest extends FlatSpec with Matchers with MockFactory with TestEventGenerator with BeforeAndAfter {
  val DOMAIN = "tested.domain"
  val STREAM = "tested.stream"
  val REQ_ID = "tested.req.id"
  val EVT_ID = "my_id"
  val NOW: Instant = Instant.EPOCH
  val INPUT_EVENT_INFO: EventInfo = newEventInfo(NOW, DOMAIN, STREAM, REQ_ID)

  val repoMock: WikibaseEntityRevRepositoryTrait = mock[WikibaseEntityRevRepositoryTrait]
  val subjectiveClock: () => Instant = mock[() => Instant]
  private def testHarness[T <: Throwable](expectExternalFailure: Option[Class[T]] = None) = {
    val genDiff = GenerateEntityDiffPatchOperation(
      wikibaseRepositoryGenerator = _ => repoMock,
      scheme = UrisSchemeFactory.forWikidataHost(DOMAIN),
      mungeOperationProvider = _ => (_, _) => 1,
      fetchRetryDelay = 1.milliseconds,
      now = subjectiveClock
    )

    val operator: AsyncFunction[MutationOperation, ResolvedOp] = new AsyncFunction[MutationOperation, ResolvedOp] {
      override def asyncInvoke(input: MutationOperation, resultFuture: ResultFuture[ResolvedOp]): Unit = genDiff.asyncInvoke(input,
        new async.ResultFuture[ResolvedOp] {

          override def completeExceptionally(throwable: Throwable): Unit = resultFuture.completeExceptionally(throwable)

          override def complete(result: Iterable[ResolvedOp]): Unit = resultFuture.complete(result.toSeq.asJava)
        }
      )
    }

    val typeInfo = TypeInformation.of(classOf[MutationOperation]: Class[MutationOperation])
    val operatorFactory = new AsyncWaitOperatorFactory[MutationOperation, ResolvedOp](operator, 5L, 10, OutputMode.ORDERED)
    val env = MockEnvironment.builder().build()
    genDiff.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 1))
    env.getExecutionConfig.registerTypeWithKryoSerializer(classOf[Patch], classOf[RDFPatchSerializer])
    // AsyncAwaitOperator will always throw Exception wrapping our own
    expectExternalFailure map { _ => classOf[Exception] } foreach env.setExpectedExternalFailureCause
    val harness = new OneInputStreamOperatorTestHarness[MutationOperation, ResolvedOp](operatorFactory, typeInfo.createSerializer(env.getExecutionConfig), env)
    harness.getExecutionConfig
    harness.open()
    harness
  }

  "a diff mutation" should "fetch 2 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val op = inputDiffOp
    val harness = sendData(op)
    harness.extractOutputValues() should contain only outputDiffEvent(op)
  }

  "a import mutation" should "fetch 1 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val op = importOp
    val harness = sendData(op)
    harness.extractOutputValues() should contain only outputImportEvent(op)
  }

  "a repo failure on an import op" should "send a failed op" in {
    val e = new ContainedException("error")
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing e

    val op = importOp
    assertFailure(e, op, sendData(op))
  }

  private def assertFailure(e: ContainedException, op: MutationOperation, harness: OneInputStreamOperatorTestHarness[MutationOperation, ResolvedOp]) = {
    val values = harness.extractOutputValues
    values.size() shouldEqual 1
    val ops: FailedOp = values.get(0) match {
      case e: FailedOp => e
    }
    ops.operation shouldBe op
    ops.exception.getClass shouldBe e.getClass
    ops.exception.getMessage shouldBe e.getMessage
  }

  "a repo single failure on an diff op" should "send a failed op" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new ContainedException("error fetching rev 2")

    val op = inputDiffOp
    assertFailure(new ContainedException("error fetching rev 2"), op, sendData(op))
  }

  "a delete mutation" should "send a delete op" in {
    val op = inputDelOp
    val harness = sendData(op)
    harness.extractOutputValues() should contain only DeleteOp(op)
  }

  "a retryable repo failure on an import op" should "send a failed op when retried too many times" in {
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")

    val op = importOp

    assertFailure(new ContainedException("Cannot fetch entity Q1 revision 1 failed 4 times, abandoning."), op, sendData(op))
  }

  "a retryable repo failure on an diff op" should "send a failed op when retried too many times" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")

    val op = inputDiffOp
    assertFailure(new ContainedException("Cannot fetch entity Q1 revision 2 failed 4 times, abandoning."), op, sendData(op))
  }

  "a retryable repo failure on an import op" should "should not prevent sending an event if temporary" in {
    for (_ <- 1 to 2) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val op = importOp
    val harness = sendData(op)
    harness.extractOutputValues() should contain only outputImportEvent(op)
  }

  "a retryable repo failure on an diff op" should "should not prevent sending an event if temporary" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    for (_ <- 1 to 3) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val op = inputDiffOp
    val harness = sendData(op)
    harness.extractOutputValues() should contain only outputDiffEvent(op)
  }

  "a 404" should "be retried while the entity is considered new" in {
    val notFoundException = new WikibaseEntityFetchException(URI.create("https://www.wikidata.org/wiki/Special:EntityData/Q1.ttl?flavor=dump&revision=1"),
      WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND)
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing notFoundException noMoreThanOnce()
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing notFoundException noMoreThanOnce()
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing notFoundException noMoreThanOnce()
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    val op = importOp
    (subjectiveClock.apply _).expects() returning NOW noMoreThanOnce()
    (subjectiveClock.apply _).expects() returning NOW.plusSeconds(1) noMoreThanOnce()
    (subjectiveClock.apply _).expects() returning NOW.plusSeconds(2)
    val harness = sendData(op)
    harness.extractOutputValues() should contain only outputImportEvent(op)
  }

  "a 404" should "be retried only while the entity is considered new" in {
    val notFoundException = new WikibaseEntityFetchException(URI.create("https://www.wikidata.org/wiki/Special:EntityData/Q1.ttl?flavor=dump&revision=1"),
      WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND)
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing notFoundException anyNumberOfTimes()
    val op = importOp
    (subjectiveClock.apply _).expects() returning NOW noMoreThanOnce()
    (subjectiveClock.apply _).expects() returning NOW.plusSeconds(1) noMoreThanOnce()
    (subjectiveClock.apply _).expects() returning NOW.plusSeconds(11)
    assertFailure(notFoundException, op, sendData(op))
  }

  "an unexpected failure fetching one of the two revisions" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")
    (repoMock.getEntityByRevision _).expects("Q1", 2).noMoreThanOnce returning statements("uri:a", "uri:b").asScala

    val op = inputDiffOp
    val harness = sendData(op, Some(classOf[IllegalArgumentException]))
    harness.extractOutputValues() shouldBe empty
  }

  "an unexpected failure fetching a single revision" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")

    val harness = sendData(importOp, Some(classOf[IllegalArgumentException]))
    harness.extractOutputValues() shouldBe empty
  }
  private def importOp = {
    FullImport("Q1", NOW, 1, NOW, INPUT_EVENT_INFO)
  }

  private def outputImportEvent(op: FullImport) = {
    EntityPatchOp(op, new Patch(statements("uri:a"), emptyList(), emptyList(), emptyList()))
  }

  private def inputDiffOp = {
    Diff("Q1", NOW, 2, 1, NOW, INPUT_EVENT_INFO)
  }

  private def inputDelOp = {
    DeleteItem("Q1", NOW, 2, NOW, INPUT_EVENT_INFO)
  }

  private def outputDiffEvent(op: Diff) = {
    EntityPatchOp(op, new Patch(statements("uri:c"), emptyList(), statements("uri:b"), emptyList()))
  }

  private def sendData[T <: Throwable](op: MutationOperation, expectedException: Option[Class[T]] = None) = {
    val harness = testHarness(expectedException)
    harness.processElement(op, 1L)
    harness.endInput()
    if (expectedException.isDefined) {
      harness.getEnvironment.getActualExternalFailureCause.get().getCause.getClass shouldEqual expectedException.get
    }
    harness
  }
}

