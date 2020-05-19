package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.openrdf.model.Statement
import org.scalatest._
import org.wikidata.query.rdf.tool.change.events.{EventsMeta, RevisionCreateEvent}
import org.wikidata.query.rdf.tool.stream.{DiffEventData, MutationEventData, RDFDataChunk}

class UpdaterPipelineIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers {

  "Updater job" should "work" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(inputEvents)
      // force 1 here so that we keep the sequence order and force Q1 rev 3 to be late
        .setParallelism(1)
      // Use punctuated WM instead of periodic in test
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[RevisionCreateEvent] {
        override def checkAndGetNextWatermark(t: RevisionCreateEvent, l: Long): Watermark = {
          val ret = t match {
            case a: Any if a.title() == "Q2" => Some(new Watermark(a.timestamp().toEpochMilli))
            case _: Any => None
          }
          ret.orNull
        }

        override def extractTimestamp(t: RevisionCreateEvent, l: Long): Long = t.timestamp().toEpochMilli
      }),
    DOMAIN,
    IncomingStreams.REV_CREATE_CONV, clock,
    // Disable any parallelism for the input collection so that order of input events are kept intact
    // (does not affect the ordering but ensure that we can detect the late event
    Some(1), Some(1))


    //this needs to be evaluated before the lambda below because of serialization issues
    val repository: MockWikibaseEntityRevRepository = getMockRepository
    UpdaterPipeline.build(UpdaterPipelineOptions(DOMAIN, REORDERING_WINDOW_LENGTH), List(source), _ => repository, OUTPUT_EVENT_UUID_GENERATOR,
        clock, OUTPUT_EVENT_STREAM_NAME)
      .saveTo(new CollectSink[EntityPatchOp](CollectSink.values.append(_)))
      .saveSpuriousEventsTo(new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_)))
      .saveLateEventsTo(new CollectSink[InputEvent](CollectSink.lateEvents.append(_)))
      .execute("test")

    CollectSink.lateEvents should contain only ignoredRevision
    CollectSink.spuriousRevEvents should contain theSameElementsAs ignoredMutations
    val expected = expectedTripleDiffs
    CollectSink.values should have size expected.size
    CollectSink.values zip expected map {
      case (EntityPatchOp(actualOp, actualData), EntityPatchOp(expectedOp, expectedData)) =>
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
        }
    }
    CollectSink.values map {_.operation} should contain theSameElementsInOrderAs expected.map {_.operation}
  }

  def asStatementsBag(chunk: RDFDataChunk): Iterable[Statement] = {
    if (chunk == null) {
      Seq()
    } else {
      rdfChunkDeser.deser(chunk, "unused").asScala
    }
  }
}
