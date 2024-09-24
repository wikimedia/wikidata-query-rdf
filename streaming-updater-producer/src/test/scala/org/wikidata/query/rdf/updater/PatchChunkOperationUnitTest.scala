package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.functions.util.ListCollector
import org.openrdf.model.URI
import org.openrdf.model.impl.ValueFactoryImpl
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.test.StatementHelper.statements
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.rdf.Patch

import java.time.{Clock, Instant, ZoneId}
import java.util
import java.util.Collections.emptyList
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

class PatchChunkOperationUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  val DOMAIN = "tested.domain"
  val STREAM = "tested.stream"
  val REQ_ID = "tested.req.id"
  val EVT_ID = "my_id"
  val NOW: Instant = Instant.EPOCH
  val INPUT_EVENT_INFO: EventInfo = newEventInfo(NOW, DOMAIN, STREAM, REQ_ID)
  val OUTPUT_EVENT_META: EventsMeta = new EventsMeta(NOW, EVT_ID, DOMAIN, STREAM, REQ_ID)
  val subgraphOneUri: URI = ValueFactoryImpl.getInstance().createURI("subgraph:one")
  val streamSubgraphOne = "stream_subgraph_one"
  val uriToStreamMap: Map[URI, String] = Map(subgraphOneUri -> streamSubgraphOne)

  "a diff operation" should "generate a diff MutationEventData" in {
    val op = buildOp()
    var chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputDiffEvent(inputDiffOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only inputDiffOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "diff"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM

    val smallChunkOp = buildOp(1)

    chunks = new util.ArrayList[MutationDataChunk]()
    smallChunkOp.flatMap(outputDiffEvent(inputDiffOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 2
    chunks.asScala.map(_.operation).toSet should contain only inputDiffOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "diff"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
  }

  "an import operation" should "generate an import MutationEventData" in {
    val op = buildOp()
    var chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputImportEvent(importOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only importOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "import"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM

    val smallChunkOp = buildOp(1)

    chunks = new util.ArrayList[MutationDataChunk]()
    smallChunkOp.flatMap(outputImportEvent(importOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 2
    chunks.asScala.map(_.operation).toSet should contain only importOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "import"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
  }

  "a delete operation" should "generate an delete MutationEventData" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputDeleteEvent(deleteOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only deleteOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "delete"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
  }

  "a reconcile operation" should "generate an import MutationEventData" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(outputReconcileEvent(reconcileOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only reconcileOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "reconcile"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
    chunks.asScala.map(_.data).map(_.asInstanceOf[DiffEventData]).map(_.getRdfAddedData.getData).mkString("\n") should include ("uri:reconciliation")
  }

  "a reconcile operation" should "support propagating a delete operation" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(DeleteOp(reconcileOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only reconcileOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "delete"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
  }

  "a diff operation" should "support propagating a delete operation" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    op.flatMap(DeleteOp(inputDiffOp), new ListCollector[MutationDataChunk](chunks))

    chunks.size() shouldBe 1
    chunks.asScala.map(_.operation).toSet should contain only inputDiffOp
    chunks.asScala.map(_.data.getOperation).toSet should contain only "delete"
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only STREAM
  }

  "subgraph URIs" should "be mapped to their corresponding streams" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()
    val collector = new ListCollector[MutationDataChunk](chunks)
    op.flatMap(outputReconcileEvent(reconcileOp, Some(subgraphOneUri)), collector)
    op.flatMap(outputImportEvent(importOp, Some(subgraphOneUri)), collector)
    op.flatMap(outputDiffEvent(inputDiffOp, Some(subgraphOneUri)), collector)
    op.flatMap(outputDeleteEvent(deleteOp, Some(subgraphOneUri)), collector)
    op.flatMap(DeleteOp(inputDiffOp, Some(subgraphOneUri)), collector)
    op.flatMap(DeleteOp(reconcileOp, Some(subgraphOneUri)), collector)
    chunks.asScala.map(_.data.getMeta.stream()).toSet should contain only streamSubgraphOne
  }

  "PatchChunkOperation" should "should fail if given an unknown subgraph URI" in {
    val op = buildOp()
    val chunks: util.List[MutationDataChunk] = new util.ArrayList[MutationDataChunk]()

    val input = outputReconcileEvent(reconcileOp, Some(ValueFactoryImpl.getInstance().createURI("unknown:uri")))
    val collector = new ListCollector[MutationDataChunk](chunks)
    assertThrows[IllegalArgumentException] {
      op.flatMap(input, collector)
    }
  }

  private def buildOp(chunkSize: Int = Int.MaxValue) = {
    new PatchChunkOperation(
      domain = DOMAIN,
      chunkSoftMaxSize = chunkSize,
      clock = Clock.fixed(NOW, ZoneId.of("UTC")),
      uniqueIdGenerator = () => EVT_ID,
      mainStream = STREAM,
      subgraphStreams = uriToStreamMap,
      mutationEventDataFactory = MutationEventDataFactory.v2()
    )
  }

  private def outputImportEvent(op: FullImport, subgraph: Option[URI] = None): EntityPatchOp = {
    EntityPatchOp(op, new Patch(statements("uri:a"), statements("uri:shared"), emptyList(), emptyList()), subgraph)
  }

  private def importOp: FullImport = {
    FullImport("Q1", NOW, 1, NOW, INPUT_EVENT_INFO)
  }

  private def deleteOp: DeleteItem = {
    DeleteItem("Q1", NOW, 1, NOW, INPUT_EVENT_INFO)
  }

  private def reconcileOp: Reconcile = {
    Reconcile("Q1", NOW, 1, NOW, INPUT_EVENT_INFO)
  }

  private def inputDiffOp: Diff = {
    Diff("Q1", NOW, 2, 1, NOW, INPUT_EVENT_INFO)
  }

  private def outputDiffEvent(op: Diff, subgraph: Option[URI] = None): EntityPatchOp = {
    EntityPatchOp(op, new Patch(statements("uri:c"), emptyList(), statements("uri:b"), emptyList()), subgraph)
  }

  private def outputDeleteEvent(op: DeleteItem, subgraph: Option[URI] = None): SuccessfulOp = {
    DeleteOp(op, subgraph)
  }

  private def outputReconcileEvent(op: Reconcile, subgraph: Option[URI] = None): SuccessfulOp = {
    ReconcileOp(op, new Patch(statements("uri:reconciliation"), emptyList(), emptyList(), emptyList()), subgraph)
  }
}
