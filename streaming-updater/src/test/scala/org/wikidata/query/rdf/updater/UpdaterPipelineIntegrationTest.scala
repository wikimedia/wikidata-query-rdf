package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest._
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent

class UpdaterPipelineIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers {

  "Updater job" should "work" in {
    implicit val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    UpdaterPipeline.build(UpdaterPipelineOptions(DOMAIN, REORDERING_WINDOW_LENGTH), List(source), _ => repository, clock)
      .saveTo(new CollectSink[ResolvedOp](CollectSink.values.append(_)))
      .saveSpuriousEventsTo(new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_)))
      .saveLateEventsTo(new CollectSink[InputEvent](CollectSink.lateEvents.append(_)))
      .execute("test")

    CollectSink.lateEvents should contain only ignoredRevision
    CollectSink.values should contain theSameElementsInOrderAs expectedTripleDiffs
    CollectSink.spuriousRevEvents should contain theSameElementsAs ignoredMutations
  }
}
