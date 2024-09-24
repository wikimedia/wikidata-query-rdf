package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.scala._
import org.openrdf.model.Statement
import org.scalatest._
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.tool.change.events.{EventsMeta, PageDeleteEvent, ReconcileEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinitions, SubgraphDefinitionsParser}
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineGeneralConfig}

import java.time.Instant
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


class UpdaterPipelineIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers {
  private val subgraphCopyStream = "subgraph-copy-stream"
  private val pipelineOptions: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = DOMAIN,
    jobName = "test updater job",
    entityNamespaces = ENTITY_NAMESPACES,
    entityDataPath = "/wiki/Special:EntityData/",
    reorderingWindowLengthMs = REORDERING_WINDOW_LENGTH,
    generateDiffTimeout = Int.MaxValue,
    wikibaseRepoThreadPoolSize = 10,
    httpClientConfig = HttpClientConfig(None, None, "my user-agent"),
    urisScheme = UrisSchemeFactory.forWikidataHost(DOMAIN),
    acceptableMediawikiLag = 10 seconds,
    outputMutationSchema = "v2"
  )

  private val resolver: IncomingStreams.EntityResolver = (_, title, _) => title

  private val subgraphDef: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    this.getClass.getResourceAsStream(this.getClass.getSimpleName + "-subgraphs.yaml"))
  private val subgraphAssigner: SubgraphAssigner = new SubgraphAssigner(subgraphDef)

  "Updater job" should "work" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)

    val revCreateSource: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(revCreateEvents)
      // force 1 here so that we keep the sequence order and force Q1 rev 3 to be late
      .setParallelism(1)
      // Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[RevisionCreateEvent]()),
      URIS,
      IncomingStreams.REV_CREATE_CONV, clock, resolver, None)

    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository

    UpdaterPipeline.configure(pipelineOptions, List(revCreateSource), CollectSink.asOutputStreams, _ => repository,
      OUTPUT_EVENT_UUID_GENERATOR, clock, OUTPUT_EVENT_STREAM_NAME, subgraphAssigner)

    env.execute("test")

    CollectSink.lateEvents should contain only ignoredRevision
    CollectSink.spuriousRevEvents should contain theSameElementsAs ignoredMutations
    val expected = expectedOperations
    CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME) should have size expected.size
    CollectSink.outputForStream(subgraphCopyStream) should have size expected.size
    compareStatements(expected)
    CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME) map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
    CollectSink.outputForStream(subgraphCopyStream) map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
  }

  "Updater job" should "work with deletes" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)

    val revCreateSourceForDeleteTest: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(revCreateEventsForPageDeleteTest)
      // force 1 here so that we keep the sequence order and force Q1 rev 3 to be late
      .setParallelism(1)
      // Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[RevisionCreateEvent]()),
      URIS,
      IncomingStreams.REV_CREATE_CONV, clock, resolver, None)

    val pageDeleteSource: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(pageDeleteEvents)
      .setParallelism(1)
      //       Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[PageDeleteEvent]()),
      URIS,
      IncomingStreams.PAGE_DEL_CONV, clock, resolver, None)


    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository

    UpdaterPipeline.configure(pipelineOptions,
      List(revCreateSourceForDeleteTest, pageDeleteSource),
      CollectSink.asOutputStreams,
      _ => repository, OUTPUT_EVENT_UUID_GENERATOR,
      clock, OUTPUT_EVENT_STREAM_NAME, subgraphAssigner)
    env.execute("test")

    val expected = expectedOperationsForPageDeleteTest

    CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME) should have size expected.size
    CollectSink.outputForStream(subgraphCopyStream) should have size expected.size
    compareStatements(expected)
    CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME) map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
    CollectSink.outputForStream(subgraphCopyStream) map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
  }

  "Updater job" should "support reconciliation events" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)

    val revCreateEventStream: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(revCreateEventsForReconcileTest)
      // force 1 here so that we keep the sequence order and force Q1 rev 3 to be late
      .setParallelism(1)
      // Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[RevisionCreateEvent]()),
      URIS,
      IncomingStreams.REV_CREATE_CONV, clock, resolver, None)

    val reconcileEventsStream: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(reconcileEvents)
      // force 1 here so that we keep the sequence order and force Q1 rev 3 to be late
      .setParallelism(1)
      // Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[ReconcileEvent]()),
      URIS,
      IncomingStreams.RECONCILIATION_CONV, clock, resolver, None)

    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository

    UpdaterPipeline.configure(pipelineOptions,
      List(revCreateEventStream, reconcileEventsStream),
      CollectSink.asOutputStreams,
      _ => repository, OUTPUT_EVENT_UUID_GENERATOR,
      clock, OUTPUT_EVENT_STREAM_NAME, subgraphAssigner)
    env.execute("test")

    val expected = expectedReconcile
    val mainOutput = CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME)
    val subgraphCopyOutput = CollectSink.outputForStream(subgraphCopyStream)
    Seq(mainOutput, subgraphCopyOutput).foreach { output =>
      val (actualReconcile, actualData) = output find  {_.operation.isInstanceOf[Reconcile]} match {
        case Some(MutationDataChunk(operation: Reconcile, data: DiffEventData)) => (operation, data)
        case _ => fail("Expected a reconcile operation with a DiffEventData data object")
      }
      actualReconcile shouldBe expected.operation
      assertSameDiffData(actualData, expected.data.asInstanceOf[DiffEventData])
    }

  }

  private def compareStatements(expected: Seq[MutationDataChunk]) = {
    CollectSink.outputForStream(OUTPUT_EVENT_STREAM_NAME) zip expected map {
      case (MutationDataChunk(actualOp, actualData), MutationDataChunk(expectedOp, expectedData)) =>
        // We don't compare directly expectedData vs actualData because the generated RDF data may be different in its serialized form
        // even though the resulting graph is identical. This might be because of the ordering or the mime type.
        def asComparableTuple(op: MutationOperation, data: MutationEventData): (MutationOperation, String, Instant, EventsMeta, String, Long, Int, Int) = {
          (op, data.getEntity, data.getEventTime, data.getMeta, data.getOperation, data.getRevision, data.getSequence, data.getSequenceLength)
        }

        asComparableTuple(actualOp, actualData) should equal(asComparableTuple(expectedOp, expectedData))
        (actualData, expectedData) match {
          case (actualDiff: DiffEventData, expectedDiff: DiffEventData) =>
            assertSameDiffData(actualDiff, expectedDiff)
          case _ =>
            // nothing to compare for the delete operation
        }
    }
  }

  private def assertSameDiffData(actualDiff: DiffEventData, expectedDiff: DiffEventData) = {
    asStatementsBag(actualDiff.getRdfAddedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfAddedData)
    asStatementsBag(actualDiff.getRdfDeletedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfDeletedData)
    asStatementsBag(actualDiff.getRdfLinkedSharedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfLinkedSharedData)
    asStatementsBag(actualDiff.getRdfUnlinkedSharedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfUnlinkedSharedData)
  }

  def asStatementsBag(chunk: RDFDataChunk): Iterable[Statement] = {
    if (chunk == null) {
      Seq()
    } else {
      rdfChunkDeser.deser(chunk, "unused").asScala
    }
  }

}
