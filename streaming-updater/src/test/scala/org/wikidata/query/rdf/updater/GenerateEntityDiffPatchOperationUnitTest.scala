package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant, ZoneOffset}
import java.util

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.ExecutionException

import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector
import org.apache.flink.streaming.util.MockStreamingRuntimeContext
import org.apache.flink.util.ExceptionUtils
import org.openrdf.rio.{RDFFormat, RDFWriterRegistry}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.test.StatementHelper.statements
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.stream.{DiffEventData, MutationEventData, RDFChunkSerializer, RDFDataChunk}

class GenerateEntityDiffPatchOperationUnitTest extends FlatSpec with Matchers with MockFactory with TestEventGenerator {
  val DOMAIN = "tested.domain"
  val STREAM = "tested.stream"
  val REQ_ID = "tested.req.id"
  val EVT_ID = "my_id"
  val NOW: Instant = Instant.EPOCH
  val OUTPUT_EVENT_META: EventsMeta = new EventsMeta(NOW, EVT_ID, DOMAIN, STREAM, REQ_ID)
  val INPUT_EVENT_META: EventsMeta = newEventMeta(NOW, DOMAIN, STREAM, REQ_ID)
  val MIME: String = RDFFormat.TURTLE.getDefaultMIMEType

  val chunkSerializer: RDFChunkSerializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance())
  val chunkSize: Int = Int.MaxValue
  val repoMock: WikibaseEntityRevRepositoryTrait = mock[WikibaseEntityRevRepositoryTrait]

  private def operator: GenerateEntityDiffPatchOperation = {
    val operator = GenerateEntityDiffPatchOperation(
      wikibaseRepositoryGenerator = _ => repoMock,
      uniqueIdGenerator = () => EVT_ID,
      domain = DOMAIN,
      clock = Clock.fixed(NOW, ZoneOffset.UTC),
      stream = STREAM,
      chunkSoftMaxSize = chunkSize,
      mungeOperationProvider = _ => (_, _) => 1)
    operator.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 1))
    operator
  }

  "a diff mutation" should "fetch 2 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = inputDiffOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only outputDiffEvent(op)
  }

  "a import mutation" should "fetch 1 entity revisions" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = importOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only outputImportEvent(op)
  }

  "a repo failure on an import op" should "send a failed op" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new ContainedException("error")

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = importOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only FailedOp(op, new ContainedException("error").toString)
  }

  "a repo single failure on an diff op" should "send a failed op" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new ContainedException("error fetching rev 2")

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = inputDiffOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only FailedOp(op, new ContainedException("error fetching rev 2").toString)
  }

  "a retryable repo failure on an import op" should "send a failed op when retried too many times" in {
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = importOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only FailedOp(op, new ContainedException("Cannot fetch entity Q1 revision 1 failed 4 times, abandoning.").toString)
  }

  "a retryable repo failure on an diff op" should "send a failed op when retried too many times" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala
    for (_ <- 1 to 4) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = inputDiffOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only FailedOp(op, new ContainedException("Cannot fetch entity Q1 revision 2 failed 4 times, abandoning.").toString)
  }

  "a retryable repo failure on an import op" should "should not prevent sending an event if temporary" in {
    for (_ <- 1 to 2) (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new RetryableException("error")
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a").asScala

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = importOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only outputImportEvent(op)
  }

  "a retryable repo failure on an diff op" should "should not prevent sending an event if temporary" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) returning statements("uri:a", "uri:b").asScala
    for (_ <- 1 to 3) (repoMock.getEntityByRevision _).expects("Q1", 2) throwing new RetryableException("error fetching rev 2")
    (repoMock.getEntityByRevision _).expects("Q1", 2) returning statements("uri:a", "uri:c").asScala

    val collected: util.List[ResolvedOp] = new util.ArrayList[ResolvedOp]()
    val op = inputDiffOp
    operator.flatMap(op, new ListCollector[ResolvedOp](collected))
    collected should contain only outputDiffEvent(op)
  }

  "an unexpected failure fetching one of the two revisions" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")
    (repoMock.getEntityByRevision _).expects("Q1", 2).noMoreThanOnce returning statements("uri:a", "uri:b").asScala

    intercept[IllegalArgumentException] {
      operator.flatMap(inputDiffOp, new DiscardingOutputCollector[ResolvedOp]())
    }.getMessage should equal("bug")
  }

  "an unexpected failure fetching a single revision" should "break the pipeline" in {
    (repoMock.getEntityByRevision _).expects("Q1", 1) throwing new IllegalArgumentException("bug")

    intercept[IllegalArgumentException] {
      operator.flatMap(importOp, new DiscardingOutputCollector[ResolvedOp]())
    }.getMessage should equal("bug")
  }

  private def importOp = {
    FullImport("Q1", NOW, 1, NOW, INPUT_EVENT_META)
  }

  private def outputImportEvent(op: FullImport) = {
    // scalastyle:off null
    EntityPatchOp(op, new DiffEventData(OUTPUT_EVENT_META, "Q1", 1, NOW, 0, 1, MutationEventData.IMPORT_OPERATION,
      chunkSerializer.serializeAsChunks(statements("uri:a"), MIME, chunkSize).get(0),
      null, null, null))
    // scalastyle:on null
  }

  private def inputDiffOp = {
    Diff("Q1", NOW, 2, 1, NOW, INPUT_EVENT_META)
  }

  private def outputDiffEvent(op: Diff) = {
    // scalastyle:off null
    EntityPatchOp(op, new DiffEventData(OUTPUT_EVENT_META, "Q1", 2, NOW, 0, 1, MutationEventData.DIFF_OPERATION,
      chunkSerializer.serializeAsChunks(statements("uri:c"), MIME, chunkSize).get(0),
      chunkSerializer.serializeAsChunks(statements("uri:b"), MIME, chunkSize).get(0),
      null, null))
    // scalastyle:on null
  }
}

