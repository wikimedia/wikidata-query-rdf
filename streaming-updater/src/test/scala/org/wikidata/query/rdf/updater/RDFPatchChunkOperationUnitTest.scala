package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant, ZoneId}
import java.util
import java.util.Collections.emptyList
import java.util.function.Supplier

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.apache.flink.api.common.functions.util.ListCollector
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.test.StatementHelper.statements
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.rdf.RDFPatch

class RDFPatchChunkOperationUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  val DOMAIN = "tested.domain"
  val STREAM = "tested.stream"
  val REQ_ID = "tested.req.id"
  val EVT_ID = "my_id"
  val NOW: Instant = Instant.EPOCH
  val INPUT_EVENT_META: EventsMeta = newEventMeta(NOW, DOMAIN, STREAM, REQ_ID)
  val OUTPUT_EVENT_META: EventsMeta = new EventsMeta(NOW, EVT_ID, DOMAIN, STREAM, REQ_ID)
  val outputMetaSupplier: Supplier[EventsMeta] = new Supplier[EventsMeta] {
    override def get(): EventsMeta = OUTPUT_EVENT_META
  }

  "a diff operation" should "generate a diff MutationEventData" in {
    val op = buildOp()
    var chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputDiffEvent(inputDiffOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only inputDiffOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "diff"

    val smallChunkOp = buildOp(1)

    chunks = new util.ArrayList[MutationDataChunk]()
    smallChunkOp.flatMap(outputDiffEvent(inputDiffOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 2
    chunks.asScala.map(_.operation).toSet should contain only inputDiffOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "diff"
  }

  "an import operation" should "generate an import MutationEventData" in {
    val op = buildOp()
    var chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputImportEvent(importOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only importOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "import"

    val smallChunkOp = buildOp(1)

    chunks = new util.ArrayList[MutationDataChunk]()
    smallChunkOp.flatMap(outputImportEvent(importOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 2
    chunks.asScala.map(_.operation).toSet should contain only importOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "import"
  }

  private def buildOp(chunkSize: Int = Int.MaxValue) = {
    new RDFPatchChunkOperation(
      domain = DOMAIN,
      chunkSoftMaxSize = chunkSize,
      clock = Clock.fixed(NOW, ZoneId.of("UTC")),
      uniqueIdGenerator = () => EVT_ID,
      stream = STREAM
    )
  }

  private def outputImportEvent(op: FullImport) = {
    EntityPatchOp(op, new RDFPatch(statements("uri:a"), statements("uri:shared"), emptyList(), emptyList()))
  }

  private def importOp = {
    FullImport("Q1", NOW, 1, NOW, INPUT_EVENT_META)
  }


  private def inputDiffOp: Diff = {
    Diff("Q1", NOW, 2, 1, NOW, INPUT_EVENT_META)
  }

  private def outputDiffEvent(op: Diff): EntityPatchOp = {
    EntityPatchOp(op, new RDFPatch(statements("uri:c"), emptyList(), statements("uri:b"), emptyList()))
  }
}
