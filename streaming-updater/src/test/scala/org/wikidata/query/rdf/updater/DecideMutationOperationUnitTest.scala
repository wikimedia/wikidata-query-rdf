package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.{ProcessOperator, StreamMap}
import org.apache.flink.streaming.util.{KeyedOneInputStreamOperatorTestHarness, OneInputStreamOperatorTestHarness}
import org.scalatest.{FlatSpec, Matchers}

class DecideMutationOperationUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  "a mapper" should "decide what operation to apply to the graph" in {
    val operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, AllMutationOperation](
      new StreamMap[InputEvent, AllMutationOperation](new DecideMutationOperation()), inputEventKeySelector, TypeInformation.of(classOf[String]))
    operator.open()
    operator.processElement(newInputEventRecord("Q1", 1, 1))
    operator.processElement(newInputEventRecord("Q1", 3, 2))
    operator.processElement(newInputEventRecord("Q1", 2, 3)) // spurious
    operator.processElement(newInputEventRecord("Q2", 1, 1))
    operator.processElement(newInputEventRecord("Q2", 2, 2))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", Instant.ofEpochMilli(1), 1))
    expectedOutput += newRecord(Diff("Q1", Instant.ofEpochMilli(2), 3, 1))
    expectedOutput += newRecord(IgnoredMutation("Q1", Instant.ofEpochMilli(3), 2, Rev("Q1", Instant.ofEpochMilli(3), 2)))
    expectedOutput += newRecord(FullImport("Q2", Instant.ofEpochMilli(1), 1))
    expectedOutput += newRecord(Diff("Q2", Instant.ofEpochMilli(2), 2, 1))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  "a process function" should "re-route spurious events to a side output" in {
    val operator = new OneInputStreamOperatorTestHarness[AllMutationOperation, MutationOperation](
      new ProcessOperator[AllMutationOperation, MutationOperation](new RouteIgnoredMutationToSideOutput()))
    operator.open()
    operator.processElement(newRecord(FullImport("Q1", Instant.ofEpochMilli(1), 1)))
    operator.processElement(newRecord(Diff("Q1", Instant.ofEpochMilli(2), 3, 1)))
    operator.processElement(newRecord(IgnoredMutation("Q1", Instant.ofEpochMilli(3), 2, Rev("Q1", Instant.ofEpochMilli(3), 2))))

    val expectedOutput = new ListBuffer[Any]
    val expectedSideOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", Instant.ofEpochMilli(1), 1))
    expectedOutput += newRecord(Diff("Q1", Instant.ofEpochMilli(2), 3, 1))
    expectedSideOutput += newRecord(IgnoredMutation("Q1", Instant.ofEpochMilli(3), 2, Rev("Q1", Instant.ofEpochMilli(3), 2)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    (decodeEvents(operator.getSideOutput(DecideMutationOperation.SPURIOUS_REV_EVENTS).toArray()) should contain
      theSameElementsAs decodeEvents(expectedSideOutput))
  }
}
