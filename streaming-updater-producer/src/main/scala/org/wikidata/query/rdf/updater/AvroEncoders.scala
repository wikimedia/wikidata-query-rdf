package org.wikidata.query.rdf.updater

import org.apache.avro.SchemaBuilder.FieldAssembler
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.flink.api.common.functions.MapFunction
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException

import scala.language.implicitConversions

class AvroEncodersSchema(fieldAssembler: FieldAssembler[Schema]) {
  def originalEventMetatada(): FieldAssembler[Schema] = {
    fieldAssembler.name("original_event_metadata").`type`(AvroEncodersSchema.events_metadata).noDefault()
  }

  def inputEvent(): FieldAssembler[Schema] = {
    fieldAssembler.name("input_event").`type`().optional().`type`(AvroEncodersSchema.input_event_schema)
  }

  def commonMetadata(): FieldAssembler[Schema] = {
    fieldAssembler.requiredString("item")
      .requiredString("event_time")
      .requiredString("ingestion_time")
      .requiredLong("revision")
  }

  def state(): FieldAssembler[Schema] = {
    fieldAssembler.name("state").`type`().optional().`type`(AvroEncodersSchema.state_schema)
  }
}

object AvroEncodersSchema {
  implicit def commonField(f: FieldAssembler[Schema]): AvroEncodersSchema = new AvroEncodersSchema(f)

  @transient lazy val events_metadata: Schema = SchemaBuilder.record("events_meta").fields()
    .requiredString("id")
    .requiredString("dt")
    .requiredString("stream")
    .optionalString("request_id")
    .optionalString("domain")
    .endRecord()

  @transient lazy val input_event_schema: Schema = SchemaBuilder.record("input_event").fields()
    .commonMetadata()
    .requiredString("event_type")
    .originalEventMetatada()
    .endRecord()

  @transient lazy val all_mutation_schema: Schema = SchemaBuilder.record("all_mutation_operation").fields()
    .commonMetadata()
    .originalEventMetatada()
    .requiredString("op_type")
    .optionalLong("from_revision")
    .optionalString("inconsistency")
    .state()
    .inputEvent()
    .endRecord()

  @transient lazy val failed_op_schema: Schema = SchemaBuilder.record("failed_op").fields()
    .name("operation").`type`(all_mutation_schema).noDefault()
    .requiredString("exception_type")
    .requiredString("exception_msg")
    .optionalString("fetch_error_type")
    .endRecord()

  @transient lazy val state_schema: Schema = SchemaBuilder.record("state").fields()
    .optionalLong("rev")
    .requiredString("status")
    .endRecord()
}

trait AvroEncoders {
  def schema(): Schema


  protected def writeMutationOperation(in: AllMutationOperation): GenericRecord = {
    val builder = new GenericRecordBuilder(AvroEncodersSchema.all_mutation_schema)
    writeBasicEventData(builder, in)
    in match {
      case e: Diff =>
        builder.set("op_type", "diff")
        builder.set("from_revision", e.fromRev)
      case _: FullImport =>
        builder.set("op_type", "import")
      case _: DeleteItem =>
        builder.set("op_type", "delete")
      case e: IgnoredMutation =>
        builder.set("op_type", "ignored")
        builder.set("inconsistency", e.inconsistencyType.name)
        builder.set("input_event", writeInputEvent(e.inputEvent))
        builder.set("state", writeState(e.state))
    }
    builder.build()
  }

  protected def writeBasicEventData(builder: GenericRecordBuilder, in: BasicEventData): Unit = {
    builder.set("item", in.item)
    builder.set("event_time", in.eventTime.toString)
    builder.set("ingestion_time", in.ingestionTime.toString)
    builder.set("revision", in.revision)
    builder.set("original_event_metadata", writeMetadata(in.originalEventMetadata))
  }

  protected def writeMetadata(metadata: EventsMeta): GenericRecord = {
    val builder = new GenericRecordBuilder(AvroEncodersSchema.events_metadata)
    builder.set("id", metadata.id())
    builder.set("dt", metadata.timestamp().toString)
    builder.set("stream", metadata.stream())
    if (metadata.requestId() != null) {
      builder.set("request_id", metadata.requestId())
    }
    if (metadata.domain() != null) {
      builder.set("domain", metadata.domain())
    }
    builder.build()
  }

  protected def writeInputEvent(in: InputEvent): GenericRecord = {
    val builder = new GenericRecordBuilder(AvroEncodersSchema.input_event_schema)
    writeBasicEventData(builder, in)

    builder.set("event_type", in match {
      case _: RevCreate => "revision-create"
      case _: PageDelete => "page-delete"
      case _: PageUndelete => "page-undelete"
    })
    builder.build()
  }

  protected def writeState(state: State): GenericRecord = {
    val builder = new GenericRecordBuilder(AvroEncodersSchema.state_schema)
    state.lastRevision.foreach(rev => builder.set("rev", rev))
    builder.set("status", state.entityStatus.toString)
    builder.build()
  }
}

object FailedOpEncoder extends MapFunction[FailedOp, GenericRecord] with AvroEncoders {
  override def map(in: FailedOp): GenericRecord = {
    val builder = new GenericRecordBuilder(AvroEncodersSchema.failed_op_schema)
    builder.set("operation", writeMutationOperation(in.operation))
    builder.set("exception_type", in.exception.getClass.getName)
    builder.set("exception_msg", in.exception.getMessage)
    in.exception match {
      case e: WikibaseEntityFetchException => builder.set("fetch_error_type", e.getErrorType.name())
      case _ =>
    }
    builder.build()
  }

  override def schema(): Schema = AvroEncodersSchema.failed_op_schema
}

object IgnoredMutationEncoder extends MapFunction[IgnoredMutation, GenericRecord] with AvroEncoders {
  override def map(in: IgnoredMutation): GenericRecord = {
    writeMutationOperation(in)
  }


  override def schema(): Schema = AvroEncodersSchema.all_mutation_schema
}

object InputEventEncoder extends MapFunction[InputEvent, GenericRecord] with AvroEncoders {
  override def map(in: InputEvent): GenericRecord = {
    writeInputEvent(in)
  }

  override def schema(): Schema = AvroEncodersSchema.input_event_schema
}
