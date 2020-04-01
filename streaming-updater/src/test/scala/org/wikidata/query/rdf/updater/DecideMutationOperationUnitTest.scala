package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.{ProcessOperator, StreamMap}
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class DecideMutationOperationUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  "a mapper" should "decide what operation to apply to the graph" in {
    val operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, AllMutationOperation](
      new StreamMap[InputEvent, AllMutationOperation](new DecideMutationOperation()), inputEventKeySelector, TypeInformation.of(classOf[String]))
    val ingestionTs = 5
    val ingestionInstant = instant(ingestionTs)
    operator.open()
    operator.processElement(newInputEventRecord("Q1", 1, 1, ingestionTs))
    operator.processElement(newInputEventRecord("Q1", 3, 2, ingestionTs))
    operator.processElement(newInputEventRecord("Q1", 2, 3, ingestionTs)) // spurious
    operator.processElement(newInputEventRecord("Q2", 1, 1, ingestionTs))
    operator.processElement(newInputEventRecord("Q2", 2, 2, ingestionTs))
    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2, Rev("Q1", instant(3), 2, ingestionInstant), ingestionInstant))
    expectedOutput += newRecord(FullImport("Q2", instant(1), 1, ingestionInstant))
    expectedOutput += newRecord(Diff("Q2", instant(2), 2, 1, ingestionInstant))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  "a process function" should "re-route spurious events to a side output" in {
    val ingestionInstant = instant(5)
    val operator = new OneInputStreamOperatorTestHarness[AllMutationOperation, MutationOperation](
      new ProcessOperator[AllMutationOperation, MutationOperation](new RouteIgnoredMutationToSideOutput()))
    operator.open()
    operator.processElement(newRecord(FullImport("Q1", instant(1), 1, ingestionInstant)))
    operator.processElement(newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant)))
    operator.processElement(newRecord(IgnoredMutation("Q1", instant(3), 2, Rev("Q1", instant(3), 2, instant(0)), ingestionInstant)))

    val expectedOutput = new ListBuffer[Any]
    val expectedSideOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant))
    expectedSideOutput += newRecord(IgnoredMutation("Q1", instant(3), 2, Rev("Q1", instant(3), 2, instant(0)), ingestionInstant))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    (decodeEvents(operator.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS).toArray()) should contain
      theSameElementsAs decodeEvents(expectedSideOutput))
  }
}
