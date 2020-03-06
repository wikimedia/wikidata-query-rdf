package org.wikidata.query.rdf.updater

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest._
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent

class UpdaterPipelineIntegrationTest extends FlatSpec with FlinkTestCluster with TestEventGenerator with Matchers {
  private val REORDERING_WINDOW_LENGTH = 60000
  private val WATERMARK_1 = REORDERING_WINDOW_LENGTH
  private val WATERMARK_2 = REORDERING_WINDOW_LENGTH*2
  private val DOMAIN = "tested.domain"

  "Updater job" should "work" in {
    implicit val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // values are collected in a static variable
    val input = Seq(
      newEvent("Q1", 2, instant(3), 0, DOMAIN),
      newEvent("Q1", 1, instant(4), 0, DOMAIN),
      newEvent("Q2", -1, instant(WATERMARK_1), 0, "unrelated.domain"), //unrelated event, test filtering and triggers watermark
      newEvent("Q1", 5, instant(WATERMARK_1 + 1), 0, DOMAIN),
      newEvent("Q1", 3, instant(5), 0, DOMAIN), // ignored late event
      newEvent("Q2", -1, instant(WATERMARK_2), 0, "unrelated.domain"), //unrelated event, test filter and triggers watermark
      newEvent("Q1", 4, instant(WATERMARK_2 + 1), 0, DOMAIN), // spurious event, rev 4 arrived after WM2 but rev5 was handled at WM1
      newEvent("Q1", 6, instant(WATERMARK_2 + 1), 0, DOMAIN)
    )
    val source: DataStream[InputEvent] = IncomingStreams.fromStream(env.fromCollection(input)
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
      IncomingStreams.REV_CREATE_CONV,
      // Disable any parallelism for the input collection so that order of input events are kept intact
      // (does not affect the ordering but ensure that we can detect the late event
      Some(1), Some(1))

    UpdaterPipeline.build(UpdaterPipelineOptions(DOMAIN, REORDERING_WINDOW_LENGTH), List(source))
      .saveTo(new CollectSink[MutationOperation](CollectSink.values.append(_)))
      .saveSpuriousEventsTo(new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_)))
      .saveLateEventsTo(new CollectSink[InputEvent](CollectSink.lateEvents.append(_)))
      .execute("test")

    CollectSink.values should contain theSameElementsInOrderAs Vector(
      FullImport("Q1", instant(4), 1),
      Diff("Q1", instant(3), 2, 1),
      Diff("Q1", instant(WATERMARK_1 + 1), 5, 2),
      Diff("Q1", instant(WATERMARK_2 + 1), 6, 5))
    CollectSink.lateEvents should contain only Rev("Q1", instant(5), 3)
    CollectSink.spuriousRevEvents should contain only IgnoredMutation("Q1", instant(WATERMARK_2 + 1), 4, Rev("Q1", instant(WATERMARK_2 + 1), 4))
  }
}
