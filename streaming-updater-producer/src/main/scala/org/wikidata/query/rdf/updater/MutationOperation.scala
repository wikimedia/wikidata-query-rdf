package org.wikidata.query.rdf.updater

import java.time.Instant

import org.wikidata.query.rdf.tool.change.events.EventInfo

/**
 * All mutations even the spurious one
 * match MutationOperation if only valid mutations are required
 */
sealed trait AllMutationOperation extends BasicEventData {
  val originalEventInfo: EventInfo
}

sealed trait MutationOperation extends AllMutationOperation with Product with Serializable

final case class Diff(item: String, eventTime: Instant, revision: Long, fromRev: Long, ingestionTime: Instant, originalEventInfo: EventInfo)
  extends MutationOperation
final case class FullImport(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant, originalEventInfo: EventInfo)
  extends MutationOperation
final case class DeleteItem(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant, originalEventInfo: EventInfo)
  extends MutationOperation

/**
 * This mutation is issued from a "spurious" event: late event that correspond to an out-of-date revision
 */
final case class IgnoredMutation(item: String,
                                 eventTime: Instant,
                                 revision: Long,
                                 inputEvent: InputEvent,
                                 ingestionTime: Instant,
                                 inconsistencyType: Inconsistency,
                                 state: State
                                ) extends AllMutationOperation {
  override val originalEventInfo: EventInfo = inputEvent.originalEventInfo
}

sealed trait Inconsistency {
  val name: String
}
case object NewerRevisionSeen extends Inconsistency {
  override val name: String = "newer_revision_seen"
}

case object NotImplementedYet extends Inconsistency {
  override val name: String = "not_implemented_yet"
}
case object UnmatchedUndelete extends Inconsistency {
  override val name: String = "unmatched_delete"
}


