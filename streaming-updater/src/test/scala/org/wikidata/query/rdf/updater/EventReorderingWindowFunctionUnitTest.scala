package org.wikidata.query.rdf.updater

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala.function.util.ScalaProcessWindowFunctionWrapper
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{FlatSpec, Matchers}

class EventReorderingWindowFunctionUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  "EventReorderingWindowFunction" should "reorder events" in {
    val window = TumblingEventTimeWindows.of(Time.seconds(1))
    val stateDescr = UpdaterStateConfiguration.newReorderingStateDesc()
    stateDescr.initializeSerializerUnlessSet(new ExecutionConfig())
    val function = new ScalaProcessWindowFunctionWrapper(new EventReorderingWindowFunction());
    val windowFunction = new InternalIterableProcessWindowFunction[InputEvent, InputEvent, String, TimeWindow](function)
    val windowOperator = new WindowOperator[String, InputEvent, java.lang.Iterable[InputEvent], InputEvent, TimeWindow](window,
      new TimeWindow.Serializer(), inputEventKeySelector, new StringSerializer(), stateDescr, windowFunction,
      EventTimeTrigger.create(), 0L, EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG)
    val operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, InputEvent](windowOperator, inputEventKeySelector,
      TypeInformation.of(classOf[String]))
    val expectedOutput = new ListBuffer[Any]

    operator.open()
    operator.processElement(newRevCreateRecord("Q1", 2, 0, 5))
    operator.processElement(newRevCreateRecord("Q1", 1, 1, 5))
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, 5))
    operator.processElement(newRevCreateRecord("Q2", 1, 0, 5))
    operator.processElement(newPageDeleteRecord("Q2", 2, 1, 5))
    operator.processElement(newRevCreateRecord("Q2", 2, 1, 5))
    operator.processWatermark(new Watermark(1000))
    operator.processElement(newRevCreateRecord("Q2", 3, 2, 5)) // Late event

    expectedOutput += newRevCreateRecord("Q1", 1, 1, 5)
    expectedOutput += newRevCreateRecord("Q1", 2, 0, 5)
    expectedOutput += newPageDeleteRecord("Q1", 2, 2, 5)
    expectedOutput += newRevCreateRecord("Q2", 1, 0, 5)
    expectedOutput += newPageDeleteRecord("Q2", 2, 1, 5)
    expectedOutput += newRevCreateRecord("Q2", 2, 1, 5)
    expectedOutput += new Watermark(1000)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

}
