package org.wikidata.query.rdf.updater

import java.time.Instant
import java.util.Collections.emptyList

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.flink.streaming.util.MockStreamingRuntimeContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.test.StatementHelper.statements
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.rdf.RDFPatch

class GenerateEntityDiffPatchOperationUnitTest extends FlatSpec with Matchers with MockFactory with TestEventGenerator {
  val DOMAIN = "tested.domain"
  val STREAM = "tested.stream"
  val REQ_ID = "tested.req.id"
  val EVT_ID = "my_id"
  val NOW: Instant = Instant.EPOCH
  val INPUT_EVENT_META: EventsMeta = newEventMeta(NOW, DOMAIN, STREAM, REQ_ID)

  val repoMock: WikibaseEntityRevRepositoryTrait = mock[WikibaseEntityRevRepositoryTrait]

  private def operator: GenerateEntityDiffPatchOperation = {
    val operator = GenerateEntityDiffPatchOperation(
      wikibaseRepositoryGenerator = _ => repoMock,
      domain = DOMAIN,
      mungeOperationProvider = _ => (_, _) => 1)
    operator.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 1))
    operator
  }

  "a diff mutation" should "fetch 2 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val op = inputDiffOp
    operator.map(op) shouldBe outputDiffEvent(op)
  }

  "a import mutation" should "fetch 1 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val op = importOp
    operator.map(op) shouldBe outputImportEvent(op)
  }

  "a repo failure on an import op" should "send a failed op" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new ContainedException("error")

    val op = importOp
    operator.map(op) shouldBe FailedOp(op, new ContainedException("error").toString)
  }

  "a repo single failure on an diff op" should "send a failed op" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new ContainedException("error fetching rev 2")

    val op = inputDiffOp
    operator.map(op) shouldBe FailedOp(op, new ContainedException("error fetching rev 2").toString)
  }

  "a retryable repo failure on an import op" should "send a failed op when retried too many times" in {
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")

    val op = importOp
    operator.map(op) shouldBe FailedOp(op, new ContainedException("Cannot fetch entity Q1 revision 1 failed 4 times, abandoning.").toString)
  }

  "a retryable repo failure on an diff op" should "send a failed op when retried too many times" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")

    val op = inputDiffOp
    operator.map(op) shouldBe FailedOp(op, new ContainedException("Cannot fetch entity Q1 revision 2 failed 4 times, abandoning.").toString)
  }

  "a retryable repo failure on an import op" should "should not prevent sending an event if temporary" in {
    for (_ <- 1 to 2) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val op = importOp
    operator.map(op) shouldBe outputImportEvent(op)
  }

  "a retryable repo failure on an diff op" should "should not prevent sending an event if temporary" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    for (_ <- 1 to 3) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val op = inputDiffOp
    operator.map(op) shouldBe outputDiffEvent(op)
  }

  "an unexpected failure fetching one of the two revisions" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")
    (repoMock.getEntityByRevision _).expects("Q1", 2).noMoreThanOnce returning statements("uri:a", "uri:b").asScala

    intercept[IllegalArgumentException] {
      operator.map(inputDiffOp)
    }.getMessage should equal("bug")
  }

  "an unexpected failure fetching a single revision" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")

    intercept[IllegalArgumentException] {
      operator.map(importOp)
    }.getMessage should equal("bug")
  }

  private def importOp = {
    FullImport("Q1", NOW, 1, NOW, INPUT_EVENT_META)
  }

  private def outputImportEvent(op: FullImport) = {
    EntityPatchOp(op, new RDFPatch(statements("uri:a"), emptyList(), emptyList(), emptyList()))
  }

  private def inputDiffOp = {
    Diff("Q1", NOW, 2, 1, NOW, INPUT_EVENT_META)
  }

  private def outputDiffEvent(op: Diff) = {
    EntityPatchOp(op, new RDFPatch(statements("uri:c"), emptyList(), statements("uri:b"), emptyList()))
  }
}

