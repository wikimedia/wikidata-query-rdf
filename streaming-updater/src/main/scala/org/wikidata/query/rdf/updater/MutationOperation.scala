package org.wikidata.query.rdf.updater

import java.time.Instant

/**
 * All mutations even the spurious one
 * match MutationOperation if only valid mutations are required
 */
sealed trait AllMutationOperation {
  val item: String
  val eventTime: Instant
  val revision: Long
}

sealed trait MutationOperation extends AllMutationOperation with Product with Serializable

final case class Diff(item: String, eventTime: Instant, revision: Long, fromRev: Long) extends MutationOperation
final case class FullImport(item: String, eventTime: Instant, revision: Long) extends MutationOperation

/**
 * This mutation is issued from a "spurious" event: late event that correspond to an out-of-date revision
 */
final case class IgnoredMutation(item: String, eventTime: Instant, revision: Long, inputEvent: InputEvent) extends AllMutationOperation
