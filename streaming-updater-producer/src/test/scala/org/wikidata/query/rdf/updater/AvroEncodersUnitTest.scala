package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.Instant
import java.util.UUID

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.exception.ContainedException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type.ENTITY_NOT_FOUND
import org.wikidata.query.rdf.updater.EntityStatus.CREATED

class AvroEncodersUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  val item: String = "Q1"
  val revision: Long = 123
  val fromRevision: Long = 122
  val eventTime: Instant = Instant.now()
  val ingestionTime: Instant = Instant.now()
  val domain: String = "tested.domain"
  val stream: String = "tested.stream"
  val uuid: String = UUID.randomUUID().toString
  val requestId: String = UUID.randomUUID().toString
  val eventMeta: EventsMeta = new EventsMeta(eventTime, uuid, domain, stream, requestId)

  "IgnoredMutation" should "be encoded properly as an avro GenericRecord" in {
    val inputEvent = RevCreate(item, eventTime, revision, ingestionTime, eventMeta)
    val state = State(Some(revision), CREATED)
    val record = IgnoredMutationEncoder.map(IgnoredMutation("Q1", eventTime, revision, inputEvent, ingestionTime, NewerRevisionSeen, state))
    assertBasicMetadata(record)
    record.get("op_type") shouldBe "ignored"
    record.get("inconsistency") shouldBe NewerRevisionSeen.name

    val origState = record.get("state") match {
      case state: GenericRecord => state
      case _ => fail("Invalid type for state")
    }
    origState.get("rev").asInstanceOf[Long] shouldBe revision
    origState.get("status") shouldBe "CREATED"
    val origEvent = record.get("input_event") match {
      case e: GenericRecord => e
      case _ => fail("Invalid type for input_event")
    }
    assertBasicMetadata(origEvent)
    origEvent.get("event_type") shouldBe "revision-create"
    GenericData.get().validate(IgnoredMutationEncoder.schema(), record) shouldBe true
  }

  "RevCreateEvent" should "be encoded properly as an avro GenericRecord" in {
    val inputEvent = RevCreate(item, eventTime, revision, ingestionTime, eventMeta)
    val record = InputEventEncoder.map(inputEvent)
    assertBasicMetadata(record)
    record.get("event_type") shouldBe "revision-create"
    GenericData.get().validate(InputEventEncoder.schema(), record) shouldBe true
  }

  "PageDeleteEvent" should "be encoded properly as an avro GenericRecord" in {
    val inputEvent = PageDelete(item, eventTime, revision, ingestionTime, eventMeta)
    val record = InputEventEncoder.map(inputEvent)
    assertBasicMetadata(record)
    assertOriginalMeta(record.get("original_event_metadata"))
    record.get("event_type") shouldBe "page-delete"
    GenericData.get().validate(InputEventEncoder.schema(), record) shouldBe true
  }

  "FailedOp event" should "be encoded properly as an avro GenericRecord" in {
    val e = new ContainedException("problem")
    val op = FailedOp(Diff(item, eventTime, revision, fromRevision, ingestionTime, eventMeta), e)
    val record = FailedOpEncoder.map(op)
    record.get("exception_type") shouldBe e.getClass.getName
    record.get("exception_msg") shouldBe e.getMessage
    record.get("fetch_error_type") should be
    val opRecord = record.get("operation") match {
      case e: GenericRecord => e
      case _ => fail(s"invalid type ${record.get("operation").getClass}")
    }
    assertBasicMetadata(opRecord)
    opRecord.get("op_type") shouldBe "diff"
    opRecord.get("from_revision").asInstanceOf[Long] shouldBe fromRevision
    GenericData.get().validate(FailedOpEncoder.schema(), record) shouldBe true
  }

  "FailedOp event on a fetch failure" should "be encoded properly as an avro GenericRecord" in {
    val e = new WikibaseEntityFetchException(new URI("http://foo.local"), ENTITY_NOT_FOUND)
    val op = FailedOp(Diff(item, eventTime, revision, fromRevision, ingestionTime, eventMeta), e)
    val record = FailedOpEncoder.map(op)
    record.get("exception_type") shouldBe e.getClass.getName
    record.get("exception_msg") shouldBe e.getMessage
    record.get("fetch_error_type") shouldBe ENTITY_NOT_FOUND.name()
    GenericData.get().validate(FailedOpEncoder.schema(), record) shouldBe true
  }

  private def assertBasicMetadata(record: GenericRecord): Unit = {
    record.get("item") shouldEqual item
    record.get("event_time") shouldEqual eventTime.toString
    record.get("ingestion_time") shouldEqual ingestionTime.toString
    record.get("revision").asInstanceOf[Long] shouldEqual revision
    assertOriginalMeta(record.get("original_event_metadata"))
  }

  def assertOriginalMeta(v: AnyRef): Unit = {
    v match {
      case value: GenericRecord =>
        value.get("id") shouldBe uuid
        value.get("dt") shouldBe eventTime.toString
        value.get("stream") shouldBe stream
        value.get("request_id") shouldBe requestId
        value.get("domain") shouldBe domain
      case _ => fail("Unexpected type " + v.getClass)
    }
  }
}
