package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.{ProcessOperator, StreamMap}
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class DecideMutationOperationUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  val testDomain = "tested.domain"
  val testStream = "test-input-stream"
  "a mapper" should "decide what operation to apply to the graph" in {
    val operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, AllMutationOperation](
      new StreamMap[InputEvent, AllMutationOperation](new DecideMutationOperation()), inputEventKeySelector, TypeInformation.of(classOf[String]))
    val ingestionTs = 5
    val ingestionInstant = instant(ingestionTs)
    operator.open()
    operator.processElement(newInputEventRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newInputEventRecord("Q1", 3, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newInputEventRecord("Q1", 2, 3, ingestionTs, testDomain, testStream, "3")) // spurious
    operator.processElement(newInputEventRecord("Q2", 1, 1, ingestionTs, testDomain, testStream, "4"))
    operator.processElement(newInputEventRecord("Q2", 2, 2, ingestionTs, testDomain, testStream, "5"))
    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      Rev("Q1", instant(3), 2, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "3")), ingestionInstant))
    expectedOutput += newRecord(FullImport("Q2", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "4")))
    expectedOutput += newRecord(Diff("Q2", instant(2), 2, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "5")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  "a process function" should "re-route spurious events to a side output" in {
    val ingestionInstant = instant(5)
    val operator = new OneInputStreamOperatorTestHarness[AllMutationOperation, MutationOperation](
      new ProcessOperator[AllMutationOperation, MutationOperation](new RouteIgnoredMutationToSideOutput()))
    operator.open()
    operator.processElement(newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1"))))
    operator.processElement(newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2"))))
    operator.processElement(newRecord(IgnoredMutation("Q1", instant(3), 2,
      Rev("Q1", instant(3), 2, instant(0), newEventMeta(instant(3), testDomain, testStream, "3")), ingestionInstant)))

    val expectedOutput = new ListBuffer[Any]
    val expectedSideOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedSideOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      Rev("Q1", instant(3), 2, instant(0), newEventMeta(instant(3), testDomain, testStream, "3")), ingestionInstant))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    (decodeEvents(operator.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS).toArray()) should contain
      theSameElementsAs decodeEvents(expectedSideOutput))
  }
}
