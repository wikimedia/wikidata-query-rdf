package org.wikidata.query.rdf.updater

import com.google.common.annotations.VisibleForTesting
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikidata.query.rdf.tool.change.events.EventsMeta
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type
import org.wikidata.query.rdf.updater.config.UpdaterPipelineOutputStreamConfig
import org.wikimedia.eventutilities.core.SerializableClock
import org.wikimedia.eventutilities.core.event.EventStream
import org.wikimedia.eventutilities.flink.EventRowTypeInfo
import org.wikimedia.eventutilities.flink.formats.json.{JsonRowSerializationSchema, KafkaEventSerializationSchema, KafkaRecordTimestampStrategy}

import java.lang
import java.time.{Clock, Instant}
import java.util.Properties

case class EventPlatformSinkWrapper[E](sink: Sink[Row],
                                       nameAndUuid: String,
                                       mapFunction: MapFunction[E, Row],
                                       rowTypeInfo: EventRowTypeInfo,
                                       outputParallelism: Int = 1) extends SinkWrapper[E] {
  def attachStream(stream: DataStream[E]): Unit = {
    stream.map(mapFunction)(rowTypeInfo)
      .name(s"transformed-$nameAndUuid")
      .uid(s"transformed-$nameAndUuid")
      .setParallelism(outputParallelism)
      .sinkTo(sink)
      .uid(nameAndUuid)
      .name(nameAndUuid)
      .setParallelism(outputParallelism)
  }
}

class OutputEventStreamsBuilder(outputStreamsConfig: UpdaterPipelineOutputStreamConfig,
                                eventPlatformFactory: EventPlatformFactory,
                                outputParallelism: Int) {
  private def streamSuffix: String = {
    outputStreamsConfig.streamVersionSuffix match {
      case Some(v) => v
      case None => throw new IllegalArgumentException("Cannot use OutputEventStreamsBuilder without a stream version suffix")
    }
  }

  def build: OutputStreams = {
    val streamToTopic: Map[String, String] = outputStreamsConfig.subgraphKafkaTopics ++ Map(outputStreamsConfig.mainStream -> outputStreamsConfig.topic)
    val mutationSink = mutationOutput(streamToTopic)
    if (outputStreamsConfig.produceSideOutputs) {
      OutputStreams(
        mutationSink = mutationSink,
        lateEventsSink =
          Some(lapsedActionOutputStream),
        spuriousEventsSink =
          Some(spuriousEventsOutputStream),
        failedOpsSink =
          Some(failedEventOutputStream))
    } else {
      OutputStreams(mutationSink)
    }
  }


  @VisibleForTesting
  def failedEventOutputStream: EventPlatformSinkWrapper[FailedOp] = {
    prepareSideOutputStream[FailedOp](
      stream = "rdf-streaming-updater.fetch-failure",
      schemaVersion = "1.2.0",
      sideOutputDomain = outputStreamsConfig.sideOutputsDomain,
      nameAndUuid = "failed-events-output",
      mapperBuilder = FailedOpToRow)
  }

  @VisibleForTesting
  def spuriousEventsOutputStream: EventPlatformSinkWrapper[InconsistentMutation] = {
    prepareSideOutputStream[InconsistentMutation](
      stream = "rdf-streaming-updater.state-inconsistency",
      schemaVersion = "1.1.0",
      sideOutputDomain = outputStreamsConfig.sideOutputsDomain,
      nameAndUuid = "spurious-events-output",
      mapperBuilder = InconsistencyToRow)
  }

  @VisibleForTesting
  def lapsedActionOutputStream: EventPlatformSinkWrapper[InputEvent] = {
    prepareSideOutputStream[InputEvent](
      stream = "rdf-streaming-updater.lapsed-action",
      schemaVersion = "1.1.0",
      sideOutputDomain = outputStreamsConfig.sideOutputsDomain,
      nameAndUuid = "late-events-output",
      mapperBuilder = LapsedActionToRow.apply)
  }

  @VisibleForTesting
  def prepareSideOutputStream[E](stream: String,
                                         schemaVersion: String,
                                         sideOutputDomain: String,
                                         nameAndUuid: String,
                                         mapperBuilder: (String, EventRowTypeInfo, String, Option[String]) => MapFunction[E, Row]
                                ): EventPlatformSinkWrapper[E] = {
    val producerConfig = new Properties()
    outputStreamsConfig.producerProperties.foreach { case (k, v) => producerConfig.setProperty(k, v) }
    val topic = outputStreamsConfig.outputTopicPrefix.getOrElse("") + stream
    val rowTypeInfo = eventPlatformFactory.eventDataStreamFactory.rowTypeInfo(stream, schemaVersion)
    val brokers = outputStreamsConfig.sideOutputsKafkaBrokers.getOrElse(outputStreamsConfig.kafkaBrokers)

    val sink = eventPlatformFactory.eventDataStreamFactory
      .kafkaSinkBuilder(stream, schemaVersion, brokers, topic, KafkaRecordTimestampStrategy.ROW_INGESTION_TIME)
      .setKafkaProducerConfig(producerConfig)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
    EventPlatformSinkWrapper(sink, s"KafkaSink-$nameAndUuid", mapperBuilder(sideOutputDomain, rowTypeInfo, stream, outputStreamsConfig.emitterId),
      rowTypeInfo, outputParallelism)
  }


  private def mutationOutput(streamToTopic: Map[String, String]): SinkWrapper[MutationDataChunk] = {

    val versionedStreamMap: Map[String, String] = (Set(outputStreamsConfig.mainStream) ++ Set(streamToTopic.keys.toList: _*))
      .map { unVersionedStreamName =>
        unVersionedStreamName -> s"$unVersionedStreamName.$streamSuffix"
      }.toMap

    assertStreamConfigIsSane(streamToTopic, versionedStreamMap)

    val versionedStreamToTopic: Map[String, String] = streamToTopic.map {
      case (stream, topic) => versionedStreamMap(stream) -> topic
    }

    val versionedStream = versionedStreamMap(outputStreamsConfig.mainStream)
    val rowTypeInfo = eventPlatformFactory.eventDataStreamFactory.rowTypeInfo(versionedStream, MutationDataChunkToRow.schemaVersion)
    val serializationSchema = eventPlatformFactory.eventDataStreamFactory.serializer(versionedStream, MutationDataChunkToRow.schemaVersion)
    val multiStreamKafkaRecordSerializer = MultiStreamKafkaRecordSerializer(versionedStreamToTopic, outputStreamsConfig.partition,
      serializationSchema, rowTypeInfo, eventPlatformFactory.clock)

    val producerConfig = new Properties()
    val txTimeoutMs = Time.minutes(15).toMilliseconds
    producerConfig.setProperty("transaction.timeout.ms", txTimeoutMs.toString)
    producerConfig.setProperty("delivery.timeout.ms", txTimeoutMs.toString)
    outputStreamsConfig.producerProperties.foreach { case (k, v) => producerConfig.setProperty(k, v) }
    val sink = KafkaSink.builder[Row].setBootstrapServers(outputStreamsConfig.kafkaBrokers)
      .setRecordSerializer(multiStreamKafkaRecordSerializer)
      .setKafkaProducerConfig(producerConfig)
      .setTransactionalIdPrefix(s"${outputStreamsConfig.topic}:${outputStreamsConfig.partition}")
      .build()
    EventPlatformSinkWrapper(sink, "mutation-output", new MutationDataChunkToRow(rowTypeInfo, versionedStreamMap), rowTypeInfo, outputParallelism)
  }

  private def assertStreamConfigIsSane(streamToTopic: Map[String, String], versionedStreamMap: Map[String, String]): Unit = {
    val streams: Map[String, EventStream] = versionedStreamMap.mapValues(eventPlatformFactory.eventStreamFactory.createEventStream)

    streams.values.filter(eventStream => Option(eventStream.schemaTitle()).isEmpty).toList match {
      case Nil =>
      case e: Any => throw new IllegalArgumentException(s"The following streams ${e.map(_.streamName()).mkString(", ")} " +
        s"do not exist")
    }
    streams.values.filterNot(_.schemaTitle().equals(MutationDataChunkToRow.schemaTitle)).toList match {
      case Nil =>
      case e: Any => throw new IllegalArgumentException(s"The following streams ${e.map(_.streamName()).mkString(", ")} " +
        s"are not compatible with ${MutationDataChunkToRow.schemaTitle}")
    }

    streams.filterNot {
        case (stream, eventStream) => eventStream.topics().contains(streamToTopic(stream))
      }.keys
      .map(s => s -> streamToTopic(s))
      .toList match {
      case Nil =>
      case e: Any => throw new IllegalArgumentException(s"The following stream to topic pairs " +
        s"${e.map { case (s, t) => s"$s -> $t" }.mkString(", ")} are forbidden")
    }
  }
}

class EventMetaToRow(metaRowTypeInfo: EventRowTypeInfo, streamVersionMap: Map[String, String]) extends (EventsMeta => Row) with Serializable {
  override def apply(meta: EventsMeta): Row = {
    val metaRow = metaRowTypeInfo.createEmptyRow()
    // do not propagate meta.dt and id, let the event-platform normalization steps handle this
    metaRow.setField("request_id", meta.requestId())
    metaRow.setField("domain", meta.domain())
    // The stream is not versioned at this point, use the map to populate the stream field with the versioned stream
    metaRow.setField("stream", streamVersionMap(meta.stream()))
    metaRow
  }
}

class MutationDataChunkToRow(rowTypeInfo: EventRowTypeInfo, streamVersionMap: Map[String, String]) extends MapFunction[MutationDataChunk, Row] {
  private val rdfChunkTypeInfo: EventRowTypeInfo = rowTypeInfo.getTypeAt("rdf_added_data").asInstanceOf[EventRowTypeInfo]
  private val metaToRow = new EventMetaToRow(rowTypeInfo.getTypeAt("meta").asInstanceOf[EventRowTypeInfo], streamVersionMap)

  override def map(input: MutationDataChunk): Row = {
    val output = rowTypeInfo.createEmptyRow()
    val value = input.data
    output.setField("meta", metaToRow(value.getMeta))

    output.setField("entity_id", value.getEntity)
    output.setField("rev_id", value.getRevision)
    output.setField("dt", value.getEventTime)
    output.setField("sequence", value.getSequence.toLong)
    output.setField("sequence_length", value.getSequenceLength.toLong)
    output.setField("operation", value.getOperation)

    value match {
      case d: DiffEventData =>
        Option(d.getRdfAddedData).map(toRdfDataChunk).foreach(output.setField("rdf_added_data", _))
        Option(d.getRdfLinkedSharedData).map(toRdfDataChunk).foreach(output.setField("rdf_linked_shared_data", _))
        Option(d.getRdfDeletedData).map(toRdfDataChunk).foreach(output.setField("rdf_deleted_data", _))
        Option(d.getRdfUnlinkedSharedData).map(toRdfDataChunk).foreach(output.setField("rdf_unlinked_shared_data", _))
        output
      case _ => output
    }
  }

  private def toRdfDataChunk(chunk: RDFDataChunk): Row = {
    val output = rdfChunkTypeInfo.createEmptyRow()
    output.setField("mime_type", chunk.getMimeType)
    output.setField("data", chunk.getData)
    output
  }

}

object MutationDataChunkToRow {
  val schemaTitle: String = MutationEventData.SCHEMA_TITLE
  val schemaVersion: String = MutationEventDataV2.SCHEMA_VERSION
}

class StaticPartitioner(partition: Int) extends FlinkKafkaPartitioner[Row] {
  override def partition(record: Row, key: Array[Byte], value: Array[Byte], targetTopic: String,
                         partitions: Array[Int]): Int = partition
}

object MultiStreamKafkaRecordSerializer {
  def apply(streamToTopic: Map[String, String],
            partition: Int,
            serializationSchema: JsonRowSerializationSchema,
            rowTypeInfo: EventRowTypeInfo,
            clock: Clock): MultiStreamKafkaRecordSerializer = {
    val streamToSerializer: Map[String, KafkaEventSerializationSchema] = streamToTopic.map { case (stream, topic) =>
      stream -> new KafkaEventSerializationSchema(rowTypeInfo,
        serializationSchema,
        new SerializableClock() {
          override def get(): Instant = clock.instant()
        },
        topic,
        KafkaRecordTimestampStrategy.ROW_INGESTION_TIME,
        new StaticPartitioner(partition))
    }
    new MultiStreamKafkaRecordSerializer(streamToSerializer)
  }
}

class MultiStreamKafkaRecordSerializer(streamToSerializer: Map[String, KafkaEventSerializationSchema]
                                      ) extends KafkaRecordSerializationSchema[Row] {
  override def open(context: SerializationSchema.InitializationContext,
                    sinkContext: KafkaRecordSerializationSchema.KafkaSinkContext): Unit = {
    super.open(context, sinkContext)
    streamToSerializer.values.foreach(_.open(context, sinkContext))
  }

  override def serialize(element: Row,
                         context: KafkaRecordSerializationSchema.KafkaSinkContext,
                         timestamp: lang.Long
                        ): ProducerRecord[Array[Byte], Array[Byte]] = {
    val stream: String = element.getFieldAs[Row]("meta").getFieldAs("stream")
    streamToSerializer
      .getOrElse(stream, throw new IllegalArgumentException(s"Unknown stream $stream"))
      .serialize(element, context, timestamp)
  }
}

class BaseSideOutputToRowMapper(sideOutputDomain: String,
                                rowTypeInfo: EventRowTypeInfo,
                                stream: String,
                                emitterId: Option[String]
                               ) extends Serializable {
  private val origEventInfoRowTypeInfo: EventRowTypeInfo = rowTypeInfo.getTypeAt("original_event_info").asInstanceOf[EventRowTypeInfo]
  private val origEventInfoMetaTypeInfo: EventRowTypeInfo = rowTypeInfo.getTypeAt("original_event_info.meta").asInstanceOf[EventRowTypeInfo]
  private val metaRowTypeInfo: EventRowTypeInfo = rowTypeInfo.getTypeAt("meta").asInstanceOf[EventRowTypeInfo]

  def basicEventData(basicEventData: BasicEventData, row: Row): Unit = {
    val meta = metaRowTypeInfo.createEmptyRow()
    meta.setField("domain", sideOutputDomain)
    meta.setField("request_id", basicEventData.originalEventInfo.meta().requestId())
    meta.setField("stream", stream)
    row.setField("meta", meta)

    emitterId.foreach(row.setField("emitter_id", _))
    row.setField("item", basicEventData.item)
    row.setField("original_ingestion_dt", basicEventData.ingestionTime)
    row.setField("revision_id", basicEventData.revision)
    val origEventInfo = origEventInfoRowTypeInfo.createEmptyRow()
    row.setField("original_event_info", origEventInfo)
    origEventInfo.setField("dt", basicEventData.eventTime)
    origEventInfo.setField("$schema", basicEventData.originalEventInfo.schema())
    val origEventMeta = origEventInfoMetaTypeInfo.createEmptyRow()
    origEventInfo.setField("meta", origEventMeta)
    origEventMeta.setField("id", basicEventData.originalEventInfo.meta().id())
    origEventMeta.setField("dt", basicEventData.originalEventInfo.meta().timestamp())
    origEventMeta.setField("stream", basicEventData.originalEventInfo.meta().stream())
    origEventMeta.setField("request_id", basicEventData.originalEventInfo.meta().requestId())
    origEventMeta.setField("domain", basicEventData.originalEventInfo.meta().domain())
  }

  def writeActionTypeAndParentRevision(row: Row, inputEvent: InputEvent): Unit = {
    val (eventType, parentRevision) = inputEvent match {
      case RevCreate(_, _, _, parentRevision, _, _) => ("revision-create", parentRevision)
      case _: PageDelete => ("page-delete", None)
      case _: PageUndelete => ("page-undelete", None)
      case ReconcileInputEvent(_, _, _, ReconcileCreation, _, _) => ("reconcile-creation", None)
      case ReconcileInputEvent(_, _, _, ReconcileDeletion, _, _) => ("reconcile-deletion", None)
    }
    row.setField("action_type", eventType)
    parentRevision.foreach(r => row.setField("parent_revision_id", r))
  }
}

case class LapsedActionToRow(sideOutputDomain: String,
                             rowTypeInfo: EventRowTypeInfo,
                             stream: String,
                             emitterId: Option[String]
                            ) extends MapFunction[InputEvent, Row] with Serializable {
  private val baseMapper = new BaseSideOutputToRowMapper(sideOutputDomain, rowTypeInfo, stream, emitterId)
  override def map(value: InputEvent): Row = {
    val row = rowTypeInfo.createEmptyRow()
    baseMapper.basicEventData(value, row)
    baseMapper.writeActionTypeAndParentRevision(row, value)
    row
  }

}

case class FailedOpToRow(sideOutputDomain: String,
                         rowTypeInfo: EventRowTypeInfo,
                         stream: String,
                         emitterId: Option[String]
                        ) extends MapFunction[FailedOp, Row]  with Serializable {
  private val baseMapper = new BaseSideOutputToRowMapper(sideOutputDomain, rowTypeInfo, stream, emitterId)
  override def map(value: FailedOp): Row = {
    val row = rowTypeInfo.createEmptyRow()
    baseMapper.basicEventData(value.operation, row)
    value.operation match {
      case e: Diff =>
        row.setField("op_type", "diff")
        row.setField("from_revision_id", e.fromRev)
      case _: FullImport =>
        row.setField("op_type", "import")
      case _: DeleteItem =>
        row.setField("op_type", "delete")
      case _: Reconcile =>
        row.setField("op_type", "reconcile")
    }
    row.setField("exception_type", value.exception.getClass.getName)
    row.setField("exception_msg", value.exception.getMessage)
    row.setField("fetch_error_type", value.exception match {
      case e: WikibaseEntityFetchException => e.getErrorType.toString
      case _ => Type.UNKNOWN.toString
    })
    row
  }
}

case class InconsistencyToRow(sideOutputDomain: String,
                              rowTypeInfo: EventRowTypeInfo,
                              stream: String,
                              emitterId: Option[String]
                             ) extends MapFunction[InconsistentMutation, Row] with Serializable {
  private val baseMapper = new BaseSideOutputToRowMapper(sideOutputDomain, rowTypeInfo, stream, emitterId)
  override def map(value: InconsistentMutation): Row = {
    val row = rowTypeInfo.createEmptyRow()
    baseMapper.basicEventData(value.inputEvent, row)
    baseMapper.writeActionTypeAndParentRevision(row, value.inputEvent)
    row.setField("inconsistency", value.inconsistencyType.name)
    value.state.lastRevision.foreach(r => row.setField("state_revision_id", r))
    row.setField("state_status", value.state.entityStatus.toString)
    row
  }
}
