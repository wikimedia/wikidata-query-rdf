package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.StreamMap
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.EntityStatus._

trait MutationFixtures extends TestEventGenerator {
  val testDomain = "tested.domain"
  val testStream = "test-input-stream"
  val ingestionTs = 5
  val ingestionInstant: Instant = instant(5)
}

class MutationResolverUnitTest extends FlatSpec with Matchers with MutationFixtures with BeforeAndAfterEach {
  var operator: KeyedOneInputStreamOperatorTestHarness[String, InputEvent, AllMutationOperation] = _

  override def beforeEach(): Unit = {
    operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, AllMutationOperation](
      new StreamMap[InputEvent, AllMutationOperation](new DecideMutationWrapperOperation()), inputEventKeySelector, TypeInformation.of(classOf[String]))
    operator.open()
  }

  "DecideMutationOperation operator" should "decide what operation to apply to the graph" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 3, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 3, ingestionTs, testDomain, testStream, "3")) // spurious
    operator.processElement(newRevCreateRecordNewPage("Q2", 1, 1, ingestionTs, testDomain, testStream, "4"))
    operator.processElement(newRevCreateRecordNewPage("Q2", 2, 2, ingestionTs, testDomain, testStream, "5"))
    operator.processElement(newPageDeleteRecord("Q2", 3, 3, ingestionTs, testDomain, testStream, "6"))
    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(3), CREATED)))
    expectedOutput += newRecord(FullImport("Q2", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "4")))
    expectedOutput += newRecord(Diff("Q2", instant(2), 2, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "5")))
    expectedOutput += newRecord(DeleteItem("Q2", instant(3), 3, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "6")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

   it should "have the delete happy path" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  it should "ignore a delete for an unknown entity" in {
    operator.processElement(newPageDeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(1), 1,
      PageDelete("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")),
      ingestionInstant, NewerRevisionSeen,State(None, UNDEFINED)))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  it should "ignore a late delete" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(2), 1,
      PageDelete("Q1", instant(2), 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "test a missed revision" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "test a missed revision and a late new revision" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), DELETED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "test ignore a revision after a delete with no undelete event" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(1), DELETED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "do a full import after receiving undelete event if matching delete was properly handled" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newPageUndeleteRecord("Q1", 1, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(FullImport("Q1", instant(3), 1, ingestionInstant, newEventMeta(instant(3), testDomain, testStream, "3")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "ignore late undelete event if revisions moved forward" in {
    val undeleteEventRecordToIgnore = newPageUndeleteRecord("Q1", 1, 3, ingestionTs, testDomain, testStream, "3")
    val undeleteEventToIgnore = undeleteEventRecordToIgnore.getValue

    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(undeleteEventRecordToIgnore)
    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 1, undeleteEventToIgnore, ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "fully import entity when first seen event is undelete" in {
    operator.processElement(newPageUndeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[StreamRecord[AllMutationOperation]]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventMeta(instant(2), testDomain, testStream, "2")))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }


  it should "mark unmatched undelete event" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 1, ingestionTs, testDomain, testStream, "1"))
    val undeleteEventRecordToMarkAsUnmatched = newPageUndeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2")
    val undeleteEventToMarkAsUnmatched = undeleteEventRecordToMarkAsUnmatched.getValue
    operator.processElement(undeleteEventRecordToMarkAsUnmatched)

    val expectedOutput = new ListBuffer[StreamRecord[AllMutationOperation]]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventMeta(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(2), 2, undeleteEventToMarkAsUnmatched,
      ingestionInstant, UnmatchedUndelete, State(Some(2), CREATED)))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  class DecideMutationWrapperOperation extends RichMapFunction[InputEvent, AllMutationOperation] with LastSeenRevState {
    override def map(in: InputEvent): AllMutationOperation = new MutationResolver().map(in, entityState)
  }
}
