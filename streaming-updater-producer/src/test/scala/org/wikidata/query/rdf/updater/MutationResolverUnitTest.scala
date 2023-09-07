package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.StreamMap
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.EntityId
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
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 3, ingestionTs, testDomain, testStream, "3")) // spurious
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q2"), 1, 1, ingestionTs, testDomain, testStream, "4"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q2"), 2, 2, ingestionTs, testDomain, testStream, "5"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q2"), 3, 3, ingestionTs, testDomain, testStream, "6"))
    val expectedOutput = new ListBuffer[Any]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 3, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(3), CREATED)))
    expectedOutput += newRecord(FullImport("Q2", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "4")))
    expectedOutput += newRecord(Diff("Q2", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "5")))
    expectedOutput += newRecord(DeleteItem("Q2", instant(3), 3, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "6")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 2
  }

   it should "have the delete happy path" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 1, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "ignore a delete for an unknown entity" in {
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(1), 1,
      PageDelete("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")),
      ingestionInstant, NewerRevisionSeen,State(None, UNDEFINED)))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 0
  }

  it should "ignore a late delete" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 1, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(2), 1,
      PageDelete("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "test a missed revision" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 2, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "test a missed revision and a late new revision" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 2, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")),
      ingestionInstant, NewerRevisionSeen, State(Some(2), DELETED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "test ignore a revision after a delete with no undelete event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")),
      ingestionInstant, MissedUndelete, State(Some(1), DELETED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "do a full import after receiving undelete event if matching delete was properly handled" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 1, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(newPageUndeleteRecord(EntityId.parse("Q1"), 1, 3, ingestionTs, testDomain, testStream, "3"))

    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(DeleteItem("Q1", instant(2), 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(FullImport("Q1", instant(3), 1, ingestionInstant, newEventInfo(instant(3), testDomain, testStream, "3")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "ignore late undelete event if revisions moved forward" in {
    val undeleteEventRecordToIgnore = newPageUndeleteRecord(EntityId.parse("Q1"), 1, 3, ingestionTs, testDomain, testStream, "3")
    val undeleteEventToIgnore = undeleteEventRecordToIgnore.getValue

    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 2, ingestionTs, testDomain, testStream, "2"))
    operator.processElement(undeleteEventRecordToIgnore)
    val expectedOutput = new ListBuffer[Any]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(3), 1, undeleteEventToIgnore, ingestionInstant, NewerRevisionSeen, State(Some(2), CREATED)))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "fully import entity when first seen event is undelete" in {
    operator.processElement(newPageUndeleteRecord(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "1"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 2, ingestionTs, testDomain, testStream, "2"))

    val expectedOutput = new ListBuffer[StreamRecord[AllMutationOperation]]

    expectedOutput += newRecord(FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(Diff("Q1", instant(2), 2, 1, ingestionInstant, newEventInfo(instant(2), testDomain, testStream, "2")))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }


  it should "mark unmatched undelete event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 2, 1, ingestionTs, testDomain, testStream, "1"))
    val undeleteEventRecordToMarkAsUnmatched = newPageUndeleteRecord(EntityId.parse("Q1"), 2, 2, ingestionTs, testDomain, testStream, "2")
    val undeleteEventToMarkAsUnmatched = undeleteEventRecordToMarkAsUnmatched.getValue
    operator.processElement(undeleteEventRecordToMarkAsUnmatched)

    val expectedOutput = new ListBuffer[StreamRecord[AllMutationOperation]]
    expectedOutput += newRecord(FullImport("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "1")))
    expectedOutput += newRecord(IgnoredMutation("Q1", instant(2), 2, undeleteEventToMarkAsUnmatched,
      ingestionInstant, UnmatchedUndelete, State(Some(2), CREATED)))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs decodeEvents(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a deleted entity when nothing in the state is found" in {
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req1"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += DeleteItem("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a created entity when nothing in the state is found" in {
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileCreation, 1, ingestionTs, testDomain, testStream, "req1"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += Reconcile("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a created entity when a older revision is in the state" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileCreation, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += Reconcile("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a deleted entity when a older revision is in the state" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 1, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 1, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += DeleteItem("Q1", instant(1), 2, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a created entity with the newer revision when a newer revision is in the state" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileCreation, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += Reconcile("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a deleted entity (by reconciling it) with the newer revision when a older revision is in the state" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 2, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += Reconcile("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a created entity with the same revision when the state agrees with the event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 3, ReconcileCreation, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += Reconcile("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "reconcile a deleted entity with the same revision when the state agrees with the event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newReconcileEventRecord(EntityId.parse("Q1"), 3, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req2"))
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += DeleteItem("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += DeleteItem("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "ambiguously reconcile a deleted entity with the same revision when the state disagrees with the event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    val reconcileInputEventRecord = newReconcileEventRecord(EntityId.parse("Q1"), 3, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req2")
    val reconcileInputEvent: ReconcileInputEvent = reconcileInputEventRecord.getValue.asInstanceOf[ReconcileInputEvent]
    operator.processElement(reconcileInputEventRecord)
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += ProblematicReconciliation("Q1", instant(1), 3, reconcileInputEvent, ingestionInstant, ReconcileAmbiguousDeletion, State(Some(3), CREATED),
      DeleteItem("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2")))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "ambiguously reconcile a created entity with the same revision when the state disagrees with the event" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req2"))

    val reconcileInputEventRecord = newReconcileEventRecord(EntityId.parse("Q1"), 3, ReconcileCreation, 1, ingestionTs, testDomain, testStream, "req3")
    val reconcileInputEvent: ReconcileInputEvent = reconcileInputEventRecord.getValue.asInstanceOf[ReconcileInputEvent]
    operator.processElement(reconcileInputEventRecord)
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += DeleteItem("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    expectedOutput += ProblematicReconciliation("Q1", instant(1), 3, reconcileInputEvent, ingestionInstant, ReconcileAmbiguousCreation, State(Some(3), DELETED),
      Reconcile("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req3")))

    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 1
  }

  it should "forcibly reconcile a deleted item when passed the revision id 0" in {
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q1"), 3, 1, ingestionTs, testDomain, testStream, "req1"))
    operator.processElement(newRevCreateRecordNewPage(EntityId.parse("Q2"), 4, 1, ingestionTs, testDomain, testStream, "req2"))
    operator.processElement(newPageDeleteRecord(EntityId.parse("Q2"), 4, 1, ingestionTs, testDomain, testStream, "req3"))
    val reconcileQ1InputEventRecord = newReconcileEventRecord(EntityId.parse("Q1"), 0, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req4")
    val reconcileQ2InputEventRecord = newReconcileEventRecord(EntityId.parse("Q2"), 0, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req5")
    val reconcileQ3InputEventRecord = newReconcileEventRecord(EntityId.parse("Q3"), 0, ReconcileDeletion, 1, ingestionTs, testDomain, testStream, "req6")
    operator.processElement(reconcileQ1InputEventRecord)
    operator.processElement(reconcileQ2InputEventRecord)
    operator.processElement(reconcileQ3InputEventRecord)
    val expectedOutput = new ListBuffer[AllMutationOperation]
    expectedOutput += FullImport("Q1", instant(1), 3, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req1"))
    expectedOutput += FullImport("Q2", instant(1), 4, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req2"))
    expectedOutput += DeleteItem("Q2", instant(1), 4, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req3"))
    expectedOutput += DeleteItem("Q1", instant(1), 0, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req4"))
    expectedOutput += DeleteItem("Q2", instant(1), 0, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req5"))
    expectedOutput += DeleteItem("Q3", instant(1), 0, ingestionInstant, newEventInfo(instant(1), testDomain, testStream, "req6"))
    decodeEvents(operator.getOutput.toArray()) should contain theSameElementsInOrderAs(expectedOutput)
    operator.numKeyedStateEntries() shouldBe 0
  }

  class DecideMutationWrapperOperation extends RichMapFunction[InputEvent, AllMutationOperation] with LastSeenRevState {
    override def map(in: InputEvent): AllMutationOperation = new MutationResolver().map(in, entityState)
  }
}
