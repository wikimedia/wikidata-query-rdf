package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.collection.JavaConverters._

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.wikidata.query.rdf.tool.EntityId
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta, PageDeleteEvent, ReconcileEvent, RevisionCreateEvent, RevisionSlot}

trait TestEventGenerator {
  def newRevCreateRecordNewPage(entity: EntityId, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                                stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](RevCreate(entity, instant(eventTime), revision, None, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newRevCreateRecord(entity: EntityId, revision: Long, fromRevision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                         stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](RevCreate(entity, instant(eventTime), revision, Some(fromRevision), instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newPageDeleteRecord(entity: EntityId, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                          stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](PageDelete(entity, instant(eventTime), revision, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newPageUndeleteRecord(entity: EntityId, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                            stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](PageUndelete(entity, instant(eventTime), revision, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newReconcileEventRecord(entity: EntityId,
                              revision: Long,
                              originalAction: ReconcileOriginalAction,
                              eventTime: Long,
                              ingestionTime: Long,
                              domain: String = "tested.domain",
                              stream: String = "tested.stream",
                              requestId: String = "tested.request.id"
                             ): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](ReconcileInputEvent(entity, instant(eventTime), revision, originalAction, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newReconcileEvent(entity: EntityId,
                        revision: Long,
                        originalAction: ReconcileEvent.Action,
                        eventTime: Instant,
                        source: String = "source",
                        domain: String = "tested.domain",
                        stream: String = "tested.stream",
                        requestId: String = "tested.request.id"
                       ): ReconcileEvent = {
    new ReconcileEvent(newEventMeta(eventTime, domain, stream, requestId), "schema", entity, revision,
      source, originalAction, newEventInfo(eventTime, domain, stream, requestId))
  }

  def instant(millis: Long): Instant = {
    Instant.ofEpochMilli(millis)
  }

  def newRevCreateEvent(item: EntityId, pageId: Long, revision: Long, eventTime: Instant, namespace: Int, // scalastyle:ignore
                        domain: String, stream: String, requestId: String, revSlots: Map[String, RevisionSlot]): RevisionCreateEvent = {
    new RevisionCreateEvent(newEventInfo(eventTime, domain, stream, requestId), pageId, revision, item.toString, namespace, revSlots.asJava)
  }

  def newRevCreateEvent(item: EntityId, pageId: Long, revision: Long, fromRevision: Long, eventTime: Instant, namespace: Int, // scalastyle:ignore
                        domain: String, stream: String, requestId: String, revSlots: Map[String, RevisionSlot]): RevisionCreateEvent = {
    new RevisionCreateEvent(newEventMeta(eventTime, domain, stream, requestId), "schema", pageId, revision,
      fromRevision, item.toString, namespace, revSlots.asJava)
  }

  def newPageDeleteEvent(item: EntityId, pageId: Long, revision: Long, eventTime: Instant, namespace: Int,
                         domain: String, stream: String, requestId: String): PageDeleteEvent = {
    new PageDeleteEvent(newEventMeta(eventTime, domain, stream, requestId), "schema", pageId, revision, item.toString, namespace)
  }

  def newEventInfo(eventTime: Instant, domain: String, stream: String, requestId: String, schema: String = "schema"): EventInfo = {
    new EventInfo(newEventMeta(eventTime, domain, stream, requestId), schema)
  }

  def newEventMeta(eventTime: Instant, domain: String, stream: String, requestId: String): EventsMeta =
    new EventsMeta(eventTime, "unused", domain, stream, requestId)

  def newRecord(mutationOperation: AllMutationOperation): StreamRecord[AllMutationOperation] = {
    new StreamRecord[AllMutationOperation](mutationOperation, mutationOperation.eventTime.toEpochMilli)
  }

  def decodeEvents(iterable: Iterable[Any]): Seq[Any] = {
    iterable.map(decodeEvent)(collection.breakOut)
  }

  def decodeEvent(ev: Any): Any = {
    ev match {
      case x: StreamRecord[_] => x.getValue
      case e: Any => e
    }
  }

  def inputEventKeySelector: KeySelector[InputEvent, String] = {
    new KeySelector[InputEvent, String] {
      override def getKey(value: InputEvent): String = value.item
    }
  }
}
