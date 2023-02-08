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
final case class Reconcile(item: String, eventTime: Instant, revision: Long, ingestionTime: Instant, originalEventInfo: EventInfo)
  extends MutationOperation

/**
 * This mutation is issued from a "spurious" event: late event that correspond to an out-of-date revision
 */
sealed trait InconsistentMutation extends AllMutationOperation {
  val item: String
  val eventTime: Instant
  val revision: Long
  val inputEvent: InputEvent
  val ingestionTime: Instant
  val inconsistencyType: Inconsistency
  val state: State
  override val originalEventInfo: EventInfo = inputEvent.originalEventInfo
}

/**
 * Emitted when a mutation is being ignored because it does not match
 * what is expected by the current state of pipeline
 */
final case class IgnoredMutation(item: String,
                                 eventTime: Instant,
                                 revision: Long,
                                 inputEvent: InputEvent,
                                 ingestionTime: Instant,
                                 inconsistencyType: Inconsistency,
                                 state: State
                                ) extends InconsistentMutation

/**
 * Emitted when the reconciliation is hitting an inconsistency
 * with the current state of the pipeline
 */
final case class ProblematicReconciliation(item: String,
                                           eventTime: Instant,
                                           revision: Long,
                                           inputEvent: ReconcileInputEvent,
                                           ingestionTime: Instant,
                                           inconsistencyType: ReconciliationIssue,
                                           state: State,
                                           mutationOperation: MutationOperation
                                          ) extends InconsistentMutation

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

case object MissedUndelete extends Inconsistency {
  override val name: String = "missed_undelete"
}

sealed trait ReconciliationIssue extends Inconsistency

case object ReconcileAmbiguousDeletion extends ReconciliationIssue {
  override val name: String = "reconcile_ambiguous_deletion"
}
case object ReconcileAmbiguousCreation extends ReconciliationIssue {
  override val name: String = "reconcile_ambiguous_creation"
}


