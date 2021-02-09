package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.flink.streaming.api.scala._
import org.openrdf.model.Statement
import org.scalatest._
import org.wikidata.query.rdf.tool.change.events.{EventsMeta, PageDeleteEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.updater.config.UpdaterPipelineGeneralConfig


class UpdaterPipelineIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers {
  private val pipelineOptions: UpdaterPipelineGeneralConfig = UpdaterPipelineGeneralConfig(
    hostname = DOMAIN,
    jobName = "test updater job",
    entityNamespaces = ENTITY_NAMESPACES,
    reorderingWindowLengthMs = REORDERING_WINDOW_LENGTH,
    generateDiffTimeout = Int.MaxValue,
    wikibaseRepoThreadPoolSize = 10,
    outputOperatorNameAndUuid = "test-output-name"
  )
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
      IncomingStreams.REV_CREATE_CONV, clock)

    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository

    UpdaterPipeline.configure(pipelineOptions,
        List(revCreateSource),
        OutputStreams(
          new CollectSink[MutationDataChunk](CollectSink.values.append(_)),
          new CollectSink[InputEvent](CollectSink.lateEvents.append(_)),
          new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_))
        ),
        _ => repository, OUTPUT_EVENT_UUID_GENERATOR,
        clock, OUTPUT_EVENT_STREAM_NAME)

    env.execute("test")

    CollectSink.lateEvents should contain only ignoredRevision
    CollectSink.spuriousRevEvents should contain theSameElementsAs ignoredMutations
    val expected = expectedOperations
    CollectSink.values should have size expected.size
    compareStatements(expected)
    CollectSink.values map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
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
      IncomingStreams.REV_CREATE_CONV, clock)

  val pageDeleteSource: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(pageDeleteEvents)
      .setParallelism(1)
      //       Use punctuated WM instead of periodic in test
      .assignTimestampsAndWatermarks(watermarkStrategy[PageDeleteEvent]()),
      URIS,
      IncomingStreams.PAGE_DEL_CONV, clock)


    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository

    UpdaterPipeline.configure(pipelineOptions,
      List(revCreateSourceForDeleteTest, pageDeleteSource),
      OutputStreams(
        new CollectSink[MutationDataChunk](CollectSink.values.append(_)),
        new CollectSink[InputEvent](CollectSink.lateEvents.append(_)),
        new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_))
      ),
      _ => repository, OUTPUT_EVENT_UUID_GENERATOR,
      clock, OUTPUT_EVENT_STREAM_NAME)
    env.execute("test")

    val expected = expectedOperationsForPageDeleteTest

    CollectSink.values should have size expected.size
    compareStatements(expected)
    CollectSink.values map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}

  }

  private def compareStatements(expected: Seq[MutationDataChunk]) = {
    CollectSink.values zip expected map {
      case (MutationDataChunk(actualOp, actualData), MutationDataChunk(expectedOp, expectedData)) =>
        // We don't compare directly expectedData vs actualData because the generated RDF data may be different in its serialized form
        // even though the resulting graph is identical. This might be because of the ordering or the mime type.
        def asComparableTuple(op: MutationOperation, data: MutationEventData): (MutationOperation, String, Instant, EventsMeta, String, Long, Int, Int) = {
          (op, data.getEntity, data.getEventTime, data.getMeta, data.getOperation, data.getRevision, data.getSequence, data.getSequenceLength)
        }

        asComparableTuple(actualOp, actualData) should equal(asComparableTuple(expectedOp, expectedData))
        (actualData, expectedData) match {
          case (actualDiff: DiffEventData, expectedDiff: DiffEventData) =>
            asStatementsBag(actualDiff.getRdfAddedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfAddedData)
            asStatementsBag(actualDiff.getRdfDeletedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfDeletedData)
            asStatementsBag(actualDiff.getRdfLinkedSharedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfLinkedSharedData)
            asStatementsBag(actualDiff.getRdfUnlinkedSharedData) should contain theSameElementsAs asStatementsBag(expectedDiff.getRdfUnlinkedSharedData)
          case _ =>
            // nothing to compare for the delete operation
        }
    }
  }

  def asStatementsBag(chunk: RDFDataChunk): Iterable[Statement] = {
    if (chunk == null) {
      Seq()
    } else {
      rdfChunkDeser.deser(chunk, "unused").asScala
    }
  }

}
