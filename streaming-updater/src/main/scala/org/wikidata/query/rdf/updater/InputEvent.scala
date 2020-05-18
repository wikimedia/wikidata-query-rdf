package org.wikidata.query.rdf.updater

import java.time.Instant

import org.wikidata.query.rdf.tool.change.events.EventsMeta

sealed trait InputEvent {
  val item: String
  val eventTime: Instant
  val revision: Long
  val ingestionTime: Instant
  val originalEventMetadata: EventsMeta
}

/** Describe a new revision */
final case class Rev(item: String,
                     eventTime: Instant,
                     revision: Long,
                     ingestionTime: Instant,
                     originalEventMetadata: EventsMeta
                    ) extends InputEvent
