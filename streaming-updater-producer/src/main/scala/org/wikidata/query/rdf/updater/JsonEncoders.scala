package org.wikidata.query.rdf.updater

import java.lang
import java.time.Instant
import java.util.function.{Consumer, Supplier}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type
import org.wikimedia.eventutilities.core.event.JsonEventGenerator

class JsonEncoders(jsonEventGenerator: JsonEventGenerator, sideOutputDomain: String) {
  def lapsedActionEvent(inputEvent: InputEvent): Consumer[ObjectNode] = {
    new Consumer[ObjectNode] {
      override def accept(root: ObjectNode): Unit = {
        basicEventData(inputEvent, root)
        writeActionTypeAndParentRevision(root, inputEvent)
      }
    }
  }

  def stateInconsistencyEvent(spuriousEvent: IgnoredMutation): Consumer[ObjectNode] = {
    new Consumer[ObjectNode] {
      override def accept(root: ObjectNode): Unit = {
        basicEventData(spuriousEvent, root)
        writeActionTypeAndParentRevision(root, spuriousEvent.inputEvent)
        root.put("inconsistency", spuriousEvent.inconsistencyType.name)
        spuriousEvent.state.lastRevision.foreach(r => root.put("state_revision_id", r))
        root.put("state_status", spuriousEvent.state.entityStatus.toString)
      }
    }
  }

  def fetchFailureEvent(failedOp: FailedOp): Consumer[ObjectNode] = {
    new Consumer[ObjectNode] {
      override def accept(root: ObjectNode): Unit = {
        basicEventData(failedOp.operation, root)
        failedOp.operation match {
          case e: Diff =>
            root.put("op_type", "diff")
            root.put("from_revision_id", e.fromRev)
          case _: FullImport =>
            root.put("op_type", "import")
          case _: DeleteItem =>
            root.put("op_type", "delete")
        }
        root.put("exception_type", failedOp.exception.getClass.getName)
        root.put("exception_msg", failedOp.exception.getMessage)
        root.put("fetch_error_type", failedOp.exception match {
          case e: WikibaseEntityFetchException => e.getErrorType.toString
          case _ => Type.UNKNOWN.toString
        })
      }
    }
  }

  private def basicEventData(basicEventData: BasicEventData, root: ObjectNode): Unit = {
    val meta = root.putObject(JsonEventGenerator.META_FIELD)
    meta.put("domain", sideOutputDomain)

    root.put("item", basicEventData.item)
    root.put("original_ingestion_dt", basicEventData.ingestionTime.toString)
    root.put("revision_id", basicEventData.revision)
    val orig_event_info: ObjectNode = root.putObject("original_event_info")
    orig_event_info.put("dt", basicEventData.eventTime.toString)
    orig_event_info.put("$schema", basicEventData.originalEventInfo.schema())
    val orig_event_meta: ObjectNode = orig_event_info.putObject("meta")
    orig_event_meta.put("id", basicEventData.originalEventInfo.meta().id())
    orig_event_meta.put("dt", basicEventData.originalEventInfo.meta().timestamp().toString)
    orig_event_meta.put("stream", basicEventData.originalEventInfo.meta().stream())
    orig_event_meta.put("request_id", basicEventData.originalEventInfo.meta().requestId())
    orig_event_meta.put("domain", basicEventData.originalEventInfo.meta().domain())
  }

  private def writeActionTypeAndParentRevision(objectNode: ObjectNode, inputEvent: InputEvent): Unit = {
    val (eventType, parentRevision) = inputEvent match {
      case RevCreate(_, _, _, parentRevision, _, _) => ("revision-create", parentRevision)
      case _: PageDelete => ("page-delete", None)
      case _: PageUndelete => ("page-undelete", None)
    }
    objectNode.put("action_type", eventType)
    parentRevision.foreach(r => objectNode.put("parent_revision_id", r))
  }

  def getSerializationSchema[E](topic: String, stream: String, schema: String, recordTimeClock: Supplier[Instant]): KafkaSerializationSchema[E] = {
    new KafkaSerializationSchema[E] {
      override def serialize(element: E, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val recordTime: Instant = recordTimeClock.get()
        val eventCreator: Consumer[ObjectNode] = element match {
          case e: InputEvent => lapsedActionEvent(e)
          case e: FailedOp => fetchFailureEvent(e)
          case e: IgnoredMutation => stateInconsistencyEvent(e)
          case _ => throw new IllegalArgumentException("Unknown input type [" + element.getClass + "]")
        }
        val jsonEvent: ObjectNode = jsonEventGenerator.generateEvent(stream, schema, eventCreator, recordTime)
        val eventData: Array[Byte] = jsonEventGenerator.serializeAsBytes(jsonEvent)
        new ProducerRecord[Array[Byte], Array[Byte]](topic, null, recordTime.toEpochMilli, null, eventData) // scalastyle:ignore null
      }
    }
  }
}

object JsonEncoders {
  val lapsedActionStream: String = "rdf-streaming-updater.lapsed-action";
  val lapsedActionSchema: String = "/rdf_streaming_updater/lapsed_action/1.0.0";

  val stateInconsistencyStream: String = "rdf-streaming-updater.state-inconsistency";
  val stateInconsistencySchema: String = "/rdf_streaming_updater/state_inconsistency/1.0.0";

  val fetchFailureStream: String = "rdf-streaming-updater.fetch-failure";
  val fetchFailureSchema: String = "/rdf_streaming_updater/fetch_failure/1.0.0";
}
