package org.wikidata.query.rdf.updater

import java.time.Instant

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.wikidata.query.rdf.tool.change.events.{EventsMeta, RevisionCreateEvent}

trait TestEventGenerator {
  def newInputEventRecord(entity: String, revision: Long, eventTime: Long, ingestionTime: Long, domain: String = "tested.domain",
                          stream: String = "tested.stream", requestId: String = "tested.request.id"): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](RevCreate(entity, instant(eventTime), revision, instant(ingestionTime),
      newEventMeta(instant(eventTime), domain, stream, requestId)), eventTime)
  }

  def instant(millis: Long): Instant = {
    Instant.ofEpochMilli(millis)
  }

  def newEvent(item: String, revision: Long, eventTime: Instant, namespace: Int, domain: String, stream: String, requestId: String): RevisionCreateEvent = {
    new RevisionCreateEvent(
      newEventMeta(eventTime, domain, stream, requestId),
      revision, item, namespace)
  }

  def newEventMeta(eventTime: Instant, domain: String, stream: String, requestId: String): EventsMeta =
    new EventsMeta(eventTime, "unused", domain, stream, requestId)

  def newRecord(mutationOperation: AllMutationOperation): StreamRecord[AllMutationOperation] = {
    new StreamRecord[AllMutationOperation](mutationOperation, mutationOperation.eventTime.toEpochMilli)
  }

  def decodeEvents(iterable: Iterable[Any]): Iterable[Any] = {
    iterable.map(decodeEvent)
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
