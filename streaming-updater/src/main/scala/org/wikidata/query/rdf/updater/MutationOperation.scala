package org.wikidata.query.rdf.updater

import java.time.Instant

import org.wikidata.query.rdf.tool.change.events.EventsMeta

/**
 * All mutations even the spurious one
 * match MutationOperation if only valid mutations are required
 */
sealed trait AllMutationOperation {
  val item: String
  val eventTime: Instant
  val revision: Long
  val ingestionTime: Instant
  val originalEventMetadata: EventsMeta
}

sealed trait MutationOperation extends AllMutationOperation with Product with Serializable

final case class Diff(item: String, eventTime: Instant, revision: Long, fromRev: Long, ingestionTime: Instant, originalEventMetadata: EventsMeta)
  extends MutationOperation
final case class FullImport(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant, originalEventMetadata: EventsMeta)
  extends MutationOperation
final case class DeleteItem(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant, originalEventMetadata: EventsMeta)
  extends MutationOperation

/**
 * This mutation is issued from a "spurious" event: late event that correspond to an out-of-date revision
 */
final case class IgnoredMutation(item: String,
                                 eventTime: Instant,
                                 revision: Long,
                                 inputEvent: InputEvent,
                                 ingestionTime: Instant
                                ) extends AllMutationOperation {
  override val originalEventMetadata: EventsMeta = inputEvent.originalEventMetadata
}
