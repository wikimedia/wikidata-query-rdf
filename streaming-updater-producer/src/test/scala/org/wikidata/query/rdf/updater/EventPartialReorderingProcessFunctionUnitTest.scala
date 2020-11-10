package org.wikidata.query.rdf.updater

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{FlatSpec, Matchers}

class EventPartialReorderingProcessFunctionUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  "EventPartialReorderingProcessFunctionUnitTest" should "reorder events" in {
    val stateDescr = UpdaterStateConfiguration.newPartialReorderingStateDesc()
    stateDescr.initializeSerializerUnlessSet(new ExecutionConfig())
    val processFunction = new EventPartialReorderingProcessFunction()
    val operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, InputEvent](new KeyedProcessOperator(processFunction),
      inputEventKeySelector,
      TypeInformation.of(classOf[String]))
    val expectedOutput = new ListBuffer[Any]

    operator.open()
    // Q1:
    //  actual order: delete: 2, create:2, create:1
    //  ideal order (window): create:1 -> create:2 -> delete:2
    //  partial order: create:2 -> create:1 (will be ignored) -> delete:2
    // Q2:
    //  actual order: undelete:1 -> delete:2 -> create:1 -> create:2
    //  ideal order (window & partial): create:1 -> delete:1 -> undelete:1 -> create:2
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, 5))
    operator.processElement(newRevCreateRecord("Q1", 2, 0, 5))
    operator.processElement(newRevCreateRecord("Q1", 1, 1, 5))
    operator.processElement(newPageUndeleteRecord("Q2", 1, 2, 5))
    operator.processElement(newPageDeleteRecord("Q2", 1, 1, 5))
    operator.processElement(newRevCreateRecord("Q2", 1, 0, 5))
    operator.processElement(newRevCreateRecord("Q2", 2, 3, 5))
    operator.processWatermark(new Watermark(1000))
    operator.processElement(newRevCreateRecord("Q2", 3, 2, 5)) // Late event

    // partial re-ordering does not re-order based on revision, here rev 1 will be skipped
    // and rev 1 will generate a inconsistency
    expectedOutput += newRevCreateRecord("Q1", 2, 0, 5)
    expectedOutput += newRevCreateRecord("Q1", 1, 1, 5)
    expectedOutput += newRevCreateRecord("Q2", 1, 0, 5)
    expectedOutput += newPageDeleteRecord("Q2", 1, 1, 5)
    expectedOutput += newPageUndeleteRecord("Q2", 1, 2, 5)
    expectedOutput += newRevCreateRecord("Q2", 2, 3, 5)
    // happen at the end because fired by the timer
    expectedOutput += newPageDeleteRecord("Q1", 2, 2, 5)
    expectedOutput += new Watermark(1000)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }
}
