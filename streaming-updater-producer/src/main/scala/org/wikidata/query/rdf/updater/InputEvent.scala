package org.wikidata.query.rdf.updater

import java.time.Instant

import org.wikidata.query.rdf.tool.change.events.EventInfo

trait BasicEventData {
  val item: String
  val eventTime: Instant
  val revision: Long
  val ingestionTime: Instant
  val originalEventInfo: EventInfo
}

sealed trait InputEvent extends BasicEventData

/** Describe a new revision */
final case class RevCreate(item: String,
                           eventTime: Instant,
                           revision: Long,
                           parentRevision: Option[Long],
                           ingestionTime: Instant,
                           originalEventInfo: EventInfo
                    ) extends InputEvent

/** Describe a delete event */
final case class PageDelete(item: String,
                            eventTime: Instant,
                            revision: Long,
                            ingestionTime: Instant,
                            originalEventInfo: EventInfo
                       ) extends InputEvent

/** Describe an undelete event */
final case class PageUndelete(item: String,
                              eventTime: Instant,
                              revision: Long,
                              ingestionTime: Instant,
                              originalEventInfo: EventInfo
                           ) extends InputEvent
