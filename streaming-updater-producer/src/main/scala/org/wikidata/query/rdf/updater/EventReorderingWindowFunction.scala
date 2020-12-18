package org.wikidata.query.rdf.updater

import scala.collection.mutable.ListBuffer

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class EventReorderingWindowFunction extends ProcessWindowFunction[InputEvent, InputEvent, String, TimeWindow] {
  override def process(key: String, context: Context, inputEvents: Iterable[InputEvent], out: Collector[InputEvent]): Unit = {
    val toReorder = new ListBuffer[InputEvent]
    for (event <- inputEvents) {
      toReorder.append(event)
    }
    toReorder
      .sortBy(e => (e.item, e.revision, e.eventTime))
      .foreach(out.collect)
  }
}

object EventReorderingWindowFunction {
  val LATE_EVENTS_SIDE_OUTPUT_TAG = new OutputTag[InputEvent]("late-events")
  def attach(stream: DataStream[InputEvent],
             windowLength: Time = Time.milliseconds(60000),
             parallelism: Option[Int] = None,
             uuid: String = "EventReordering"): DataStream[InputEvent] = {
    val reorderedStream = stream
      .keyBy(_.item)
      .window(TumblingEventTimeWindows.of(windowLength))
      // NOTE: allowing lateness here is tricky to handle as it might fire the same window multiple times
      // https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#late-elements-considerations
      // hoping that this is mitigated by using BoundedOutOfOrdernessTimestampExtractor which emits late watermark so that
      // the window is triggered only after its max lateness is reached
      //.allowedLateness(Time.milliseconds(maxLateness))
      .sideOutputLateData(LATE_EVENTS_SIDE_OUTPUT_TAG)
      .process(new EventReorderingWindowFunction())
      .uid(uuid)
    parallelism.foreach(reorderedStream.setParallelism)
    reorderedStream
  }
}
