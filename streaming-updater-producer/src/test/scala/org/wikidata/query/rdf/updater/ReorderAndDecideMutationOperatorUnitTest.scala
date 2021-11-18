package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.EntityStatus.{CREATED, DELETED, UNDEFINED}

class ReorderAndDecideMutationOperatorUnitTest extends FlatSpec with Matchers with TestEventGenerator with BeforeAndAfterEach {
  val testDomain = "tested.domain"
  val testStream = "test-input-stream"
  val ingestionTs = 5
  val ingestionInstant: Instant = instant(5)

  var operator: KeyedOneInputStreamOperatorTestHarness[String, InputEvent, MutationOperation] = _

  override def beforeEach(): Unit = {
    val stateDescr = UpdaterStateConfiguration.newPartialReorderingStateDesc(true)
    stateDescr.initializeSerializerUnlessSet(new ExecutionConfig())
    val processFunction = new ReorderAndDecideMutationOperation(10, true )
    operator = new KeyedOneInputStreamOperatorTestHarness[String, InputEvent, MutationOperation](new KeyedProcessOperator(processFunction),
      inputEventKeySelector,
      TypeInformation.of(classOf[String]))
    operator.open()
  }

  "ReorderAndDecideMutationOperator operator" should "decide what operation to apply to the graph" in {
    operator.processElement(newRevCreateRecord("Q1", 3, 2, 30, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 5, ingestionTs, testDomain, testStream, "1"))
    operator.processWatermark(42)
    operator.processElement(newRevCreateRecord("Q1", 2, 1, 45, ingestionTs, testDomain, testStream, "3")) // spurious
    operator.processElement(newRevCreateRecord("Q1", 2, 1, 4, ingestionTs, testDomain, testStream, "3")) // late
    operator.processElement(newPageDeleteRecord("Q2", 3, 103, ingestionTs, testDomain, testStream, "6"))
    operator.processElement(newRevCreateRecordNewPage("Q2", 1, 100, ingestionTs, testDomain, testStream, "4"))
    operator.processElement(newRevCreateRecord("Q2", 2, 1, 102, ingestionTs, testDomain, testStream, "5"))
    operator.processWatermark(200)
    operator.close()
    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(5), 1, ingestionInstant, newEventInfo(instant(5), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(30), 3, 1, ingestionInstant, newEventInfo(instant(30), testDomain, testStream, "2")))
    expectedOutput += new Watermark(42)
    expectedOutput += newRecord(FullImport("Q2", instant(100), 1, ingestionInstant, newEventInfo(instant(100), testDomain, testStream, "4")))
    expectedOutput += newRecord(Diff("Q2", instant(102), 2, 1, ingestionInstant, newEventInfo(instant(102), testDomain, testStream, "5")))
    expectedOutput += newRecord(DeleteItem("Q2", instant(103), 3, ingestionInstant, newEventInfo(instant(103), testDomain, testStream, "6")))
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(operator.getSideOutput(ReorderAndDecideMutationOperation.LATE_EVENTS_SIDE_OUTPUT_TAG).toArray()) should contain only decodeEvent(
      newRevCreateRecord("Q1", 2, 1, 4, ingestionTs, testDomain, testStream, "3"))
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(45), 2,
      RevCreate("Q1", instant(45), 2, Some(1), ingestionInstant, newEventInfo(instant(45), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(3), CREATED))))
  }

  it should "have the delete happy path" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
  }

  it should "ignore a delete for an unknown entity" in {
    operator.processElement(newPageDeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += new Watermark(200)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(1), 1,
      PageDelete("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")),
      ingestionInstant, NewerRevisionSeen,State(None, UNDEFINED))))
  }

  it should "ignore a late delete" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processWatermark(20)
    operator.processElement(newPageDeleteRecord("Q1", 1, 25, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += new Watermark(20)
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(25), 1,
      PageDelete("Q1", instant(25), 1, ingestionInstant, newEventInfo(instant(25), testDomain, testStream, "2")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED))))
  }

  it should "test a missed revision" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "test a missed revision and a late new revision" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(25)
    operator.processElement(newRevCreateRecord("Q1", 2, 1, 40, ingestionTs, testDomain, testStream, "3"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(25)
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(40), 2,
      RevCreate("Q1", instant(40), 2, Some(1), ingestionInstant, newEventInfo(instant(40), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), DELETED))))
  }

  it should "ignore a revision after a delete with no undelete event" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecord("Q1", 2, 1, 3, ingestionTs, testDomain, testStream, "3"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, Some(1), ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(1), DELETED))))
  }

  it should "do a full import after receiving undelete event if matching delete was properly handled" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord("Q1", 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newPageUndeleteRecord("Q1", 1, 3, ingestionTs, testDomain, testStream, "3"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(FullImport("Q1", instant(3), 1, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")))
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "ignore late undelete event if revisions moved forward" in {
    val undeleteEventRecordToIgnore = newPageUndeleteRecord("Q1", 1, 25, ingestionTs, testDomain, testStream, "3")
    val undeleteEventToIgnore = undeleteEventRecordToIgnore.getValue

    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(20)
    operator.processElement(undeleteEventRecordToIgnore)
    operator.processWatermark(200)
    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(20)
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(25), 1,
      undeleteEventToIgnore, ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED))))
  }

  it should "fully import entity when first seen event is undelete" in {
    operator.processElement(newPageUndeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "reconcile an entity event" in {
    operator.processElement(newPageUndeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)
    // state should be revision created after this event
    operator.processElement(newReconcileEventRecord("Q1", 2,  ReconcileCreation, 300, ingestionTs, testDomain, testStream, "3"))
    operator.processElement(newRevCreateRecord("Q1", 3, 2, 300, ingestionTs, testDomain, testStream, "4"))
    operator.processWatermark(400)

    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)
    expectedOutput += newRecord(Reconcile("Q1", instant(300), 2, ingestionInstant, newEventInfo(instant(300), testDomain, testStream, "3")))
    expectedOutput += newRecord(Diff("Q1", instant(300), 3, 2, ingestionInstant, newEventInfo(instant(300), testDomain, testStream, "4")))
    expectedOutput += new Watermark(400)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
  }

  it should "reconcile an ambiguous entity event" in {
    operator.processElement(newPageUndeleteRecord("Q1", 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processWatermark(200)
    val reconcileRecord = newReconcileEventRecord("Q1", 2,  ReconcileDeletion, 300, ingestionTs, testDomain, testStream, "3")
    val reconcileEvent: ReconcileInputEvent = reconcileRecord.getValue.asInstanceOf[ReconcileInputEvent]
    // State should say revision 2 is deleted after this event
    operator.processElement(reconcileRecord)
    operator.processElement(newPageUndeleteRecord("Q1", 2, 300, ingestionTs, testDomain, testStream, "4"))
    operator.processWatermark(400)

    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += new Watermark(200)
    expectedOutput += newRecord(DeleteItem("Q1", instant(300), 2, ingestionInstant, newEventInfo(instant(300), testDomain, testStream, "3")))
    expectedOutput += newRecord(FullImport("Q1", instant(300), 2, ingestionInstant, newEventInfo(instant(300), testDomain, testStream, "4")))
    expectedOutput += new Watermark(400)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(ProblematicReconciliation("Q1", instant(300), 2,
        reconcileEvent, ingestionInstant, ReconcileAmbiguousDeletion, State(Some(2), CREATED),
        DeleteItem("Q1", instant(300), 2, ingestionInstant, reconcileEvent.originalEventInfo))))
  }

  it should "mark unmatched undelete event" in {
    operator.processElement(newRevCreateRecordNewPage("Q1", 2, 1, ingestionTs, testDomain, testStream, "1"))
    val undeleteEventRecordToMarkAsUnmatched = newPageUndeleteRecord("Q1", 2, 2, ingestionTs, testDomain, testStream, "2")
    val undeleteEventToMarkAsUnmatched = undeleteEventRecordToMarkAsUnmatched.getValue
    operator.processElement(undeleteEventRecordToMarkAsUnmatched)
    operator.processWatermark(200)

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += new Watermark(200)
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(newRecord(IgnoredMutation("Q1", instant(2), 2,
      undeleteEventToMarkAsUnmatched, ingestionInstant, UnmatchedUndelete, State(Some(2), CREATED))))
  }

  it should "initial import -> early page delete -> dupped rev create" in {
    val ignoredRevCreate: StreamRecord[InputEvent] = newRevCreateRecordNewPage("Q1", 1, 20,
      ingestionTs, testDomain, testStream, "dupped (backfill) rev create")
    val expectedOutput = new ListBuffer[Any]

    operator.processElement(newRevCreateRecordNewPage("Q1", 1, 0, ingestionTs, testDomain, testStream, "initial import (set state)"))
    operator.processWatermark(20)

    expectedOutput += newRecord(FullImport("Q1", instant(0), 1, ingestionInstant, newEventInfo(instant(0),
      testDomain, testStream, "initial import (set state)")))
    expectedOutput += new Watermark(20)

    // make sure the initial import state is set
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)

    operator.processElement(newPageDeleteRecord("Q1", 1, 22, ingestionTs, testDomain, testStream, "delete"))
    operator.processElement(ignoredRevCreate)
    operator.processWatermark(200)

    expectedOutput += newRecord(DeleteItem("Q1", instant(22), 1, ingestionInstant, newEventInfo(instant(22), testDomain, testStream, "delete")))
    expectedOutput += new Watermark(200)

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    decodeEvents(getSpuriousEvents) should contain only decodeEvent(
      newRecord(IgnoredMutation("Q1", instant(20), 1, ignoredRevCreate.getValue, ingestionInstant, NewerRevisionSeen, State(Some(1), CREATED))))
  }

  private def getSpuriousEvents = {
    operator.getSideOutput(ReorderAndDecideMutationOperation.SPURIOUS_REV_EVENTS).toArray
  }
}
