package org.wikidata.query.rdf.updater

import java.time.Instant

sealed trait InputEvent {
  val item: String
  val eventTime: Instant
  val revision: Long
  val ingestionTime: Instant
}

/** Describe a new revision */
final case class Rev(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant) extends InputEvent
