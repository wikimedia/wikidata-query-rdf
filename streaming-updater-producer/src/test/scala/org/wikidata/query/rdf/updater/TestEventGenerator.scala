package org.wikidata.query.rdf.updater

import java.time.Instant

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta, PageDeleteEvent, RevisionCreateEvent}

trait TestEventGenerator {
  def newRevCreateRecordNewPage(entity: String, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                                stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](RevCreate(entity, instant(eventTime), revision, None, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newRevCreateRecord(entity: String, revision: Long, fromRevision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                         stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](RevCreate(entity, instant(eventTime), revision, Some(fromRevision), instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newPageDeleteRecord(entity: String, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                          stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](PageDelete(entity, instant(eventTime), revision, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def newPageUndeleteRecord(entity: String, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                            stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](PageUndelete(entity, instant(eventTime), revision, instant(ingestionTime),
      newEventInfo(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def instant(millis: Long): Instant = {
    Instant.ofEpochMilli(millis)
  }

  def newRevCreateEvent(item: String, pageId: Long, revision: Long, eventTime: Instant, namespace: Int,
                        domain: String, stream: String, requestId: String): RevisionCreateEvent = {
    new RevisionCreateEvent(
      newEventInfo(eventTime, domain, stream, requestId),
      pageId, revision, item, namespace)
  }

  def newRevCreateEvent(item: String, pageId: Long, revision: Long, fromRevision: Long, eventTime: Instant, namespace: Int, // scalastyle:ignore
                        domain: String, stream: String, requestId: String): RevisionCreateEvent = {
    new RevisionCreateEvent(
      newEventMeta(eventTime, domain, stream, requestId), "schema", pageId, revision, fromRevision, item, namespace)
  }

  def newPageDeleteEvent(item: String, pageId: Long, revision: Long, eventTime: Instant, namespace: Int,
                         domain: String, stream: String, requestId: String): PageDeleteEvent = {
    new PageDeleteEvent(newEventMeta(eventTime, domain, stream, requestId), "schema", pageId, revision, item, namespace)
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
