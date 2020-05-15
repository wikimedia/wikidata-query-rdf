package org.wikidata.query.rdf.updater

import java.time.Instant

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.wikidata.query.rdf.tool.change.events.{EventsMeta, RevisionCreateEvent}

trait TestEventGenerator {
  def newInputEventRecord(entity: String, revision: Long, eventTime: Long, ingestionTime: Long): StreamRecord[InputEvent] = {
    new StreamRecord[InputEvent](Rev(entity, instant(eventTime), revision, instant(ingestionTime)), eventTime)
  }

  def instant(millis: Long): Instant = {
    Instant.ofEpochMilli(millis)
  }

  def newEvent(item: String, revision: Long, eventTime: Instant, namespace: Int, domain: String): RevisionCreateEvent = {
    new RevisionCreateEvent(
      new EventsMeta(eventTime, "unused", domain, "unused for now", "unused for now"),
      revision, item, namespace)
  }

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
