package org.wikidata.query.rdf.updater

import java.time.Instant

import scala.math.{abs, max}

import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.wikidata.query.rdf.tool.change.events.EventInfo
import org.wikidata.query.rdf.updater.EntityStatus.{CREATED, DELETED, UNDEFINED}

sealed class MutationResolver extends Serializable {

  def map(event: InputEvent, entityState: EntityState): AllMutationOperation = {
    event match {
      case rev: RevCreate => mutationFromRevisionCreate(rev, entityState)
      case del: PageDelete => mutationFromPageDelete(del, entityState)
      case undel: PageUndelete => mutationFromPageUndelete(undel, entityState)
      // Special case for forcing a delete via a reconcile event. It is hard to know what was the last revision of the deleted item
      // Allow clients to emit such events without a rev id so that we can forcibly delete the item from the underlying triple stores
      case reconcile @ ReconcileInputEvent(_, _, 0L, ReconcileDeletion, _, _) => forcedDeleteReconciliation(reconcile, entityState)
      case reconcile: ReconcileInputEvent => mutationFromReconcileInputEvent(reconcile, entityState)
    }
  }

  def forcedDeleteReconciliation(reconcile: ReconcileInputEvent, entityState: EntityState): AllMutationOperation = {
    entityState.clear()
    DeleteItem(reconcile.item, reconcile.eventTime, 0, reconcile.ingestionTime, reconcile.originalEventInfo)
  }

  def mutationFromReconcileInputEvent(reconcile: ReconcileInputEvent, entityState: EntityState): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitReconcileOrDelete(reconcile, entityState)
      case State(Some(stateRevision), entityStatus) =>
        emitReconcileAfterReconciliationWithState(entityState, stateRevision, entityStatus, reconcile)
    }
  }

  private def mutationFromRevisionCreate(event: RevCreate, entityState: EntityState): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitFullImport(event, entityState)
      case State(Some(rev), CREATED) if rev < event.revision => emitDiff(event, rev, entityState)
      // revision-create on top of a deleted entity (most likely the corresponding undelete event was missed)
      case state @ State(Some(rev), DELETED) if rev < event.revision => emitIgnoredMutation(event, MissedUndelete, state)
      // rest should be events for past revision (duplicate events or backfill)
      case state: State =>  emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def mutationFromPageDelete(event: PageDelete, entityState: EntityState): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(Some(rev), CREATED) if rev <= event.revision => emitPageDelete(event, entityState)
      case state: State => emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def mutationFromPageUndelete(event: PageUndelete, entityState: EntityState): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitFullImport(event, entityState)
      case State(Some(rev), DELETED) if rev == event.revision => emitFullImport(event, entityState)
      case state @ State(Some(rev), DELETED) if rev != event.revision => emitIgnoredMutation(event, UnmatchedUndelete, state)
      case state @ State(Some(rev), CREATED) if rev == event.revision => emitIgnoredMutation(event, UnmatchedUndelete, state)
      case state: State => emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def emitReconcileOrDelete(reconcile: ReconcileInputEvent, entityState: EntityState) = {
    reconcile.originalAction match {
      case ReconcileCreation =>
        emitReconcile(entityState, reconcile.item, reconcile.revision, reconcile.eventTime, reconcile.ingestionTime, reconcile.originalEventInfo)
      case ReconcileDeletion =>
        emitDelete(entityState, reconcile.item, reconcile.revision, reconcile.eventTime, reconcile.ingestionTime, reconcile.originalEventInfo)
    }
  }

  def emitReconcile(entityState: EntityState,
                    item: String,
                    revision: Long,
                    eventTime: Instant,
                    ingestionTime: Instant,
                    originalEventInfo: EventInfo): Reconcile = {
    entityState.updateRevCreate(revision)
    Reconcile(item, eventTime, revision, ingestionTime, originalEventInfo)
  }

  /**
   * Here we try to determine who is correct and has the most recent state:
   * Take the most recent revision and re-apply its action
   * If revision are equal trust the reconcile event but carry it within a ProblematicReconciliation
   * so that we can still fire an event in the side-output to figure out what to do
   */
  private def emitReconcileAfterReconciliationWithState(entityState: EntityState,
                                 stateRevision: Long,
                                 stateEntityStatus: EntityStatus.Value,
                                 reconcile: ReconcileInputEvent
                                ): AllMutationOperation = {
    val mostRecentRevision: Long = max(stateRevision, reconcile.revision)
    val reconcileEntityStatus = reconcile.originalAction match {
      case ReconcileDeletion => DELETED
      case ReconcileCreation => CREATED
    }
    val stateDisagreement: Boolean = stateEntityStatus != reconcileEntityStatus

    if (stateRevision == reconcile.revision && stateDisagreement) {
      // If both the flink state and the reconciliation event affect the same revision
      // but they require different action we prefer to follow the reconcile event
      emitProblematicReconciliation(entityState, reconcile)
    } else {
      val actionToPerform = if (stateRevision > reconcile.revision) {
        stateEntityStatus
      } else {
        reconcileEntityStatus
      }
      actionToPerform match {
        case CREATED =>
          emitReconcile(entityState, reconcile.item, mostRecentRevision, reconcile.eventTime, reconcile.ingestionTime, reconcile.originalEventInfo)
        case DELETED =>
          emitDelete(entityState, reconcile.item, mostRecentRevision, reconcile.eventTime, reconcile.ingestionTime, reconcile.originalEventInfo)
      }
    }
  }

  private def emitProblematicReconciliation(entityState: EntityState, reconcile: ReconcileInputEvent) = {
    ProblematicReconciliation(
      reconcile.item,
      reconcile.eventTime,
      reconcile.revision,
      reconcile,
      reconcile.ingestionTime,
      reconcile.originalAction match {
        case ReconcileCreation => ReconcileAmbiguousCreation
        case ReconcileDeletion => ReconcileAmbiguousDeletion
      },
      entityState.getCurrentState,
      emitReconcileOrDelete(reconcile, entityState)
    )
  }

  private def emitIgnoredMutation(event: InputEvent, inconsistencyType: Inconsistency, state: State): IgnoredMutation = {
    IgnoredMutation(event.item, event.eventTime, event.revision, event, event.ingestionTime, inconsistencyType, state)
  }

  private def emitFullImport(event: InputEvent, entityState: EntityState): FullImport = {
    entityState.updateRevCreate(event.revision)
    FullImport(event.item, event.eventTime, event.revision, event.ingestionTime, event.originalEventInfo)
  }

  private def emitDiff(event: RevCreate, prevRevision: Long, entityState: EntityState): Diff = {
    entityState.updateRevCreate(event.revision)
    Diff(event.item, event.eventTime, event.revision, prevRevision, event.ingestionTime, event.originalEventInfo)
  }

  private def emitPageDelete(event: PageDelete, entityState: EntityState): DeleteItem = {
    emitDelete(entityState, event.item, event.revision, event.eventTime, event.ingestionTime, event.originalEventInfo)
  }

  private def emitDelete(entityState: EntityState, item: String, revision: Long, eventTime: Instant, ingestionTime: Instant, originalEventInfo: EventInfo) = {
    entityState.updatePageDelete(revision)
    DeleteItem(item, eventTime, revision, ingestionTime, originalEventInfo)
  }
}

class EntityState(revState: ValueState[java.lang.Long]) {
  def updatePageDelete(revision: Long): Unit = revState.update(-revision)
  def updateRevCreate(revision: Long): Unit = revState.update(revision)
  def clear(): Unit = revState.clear()

  def getCurrentState: State = {
    Option(revState.value()) match {
      case None => State(None, UNDEFINED)
      case Some(rev) => State(Some(abs(rev)), if (rev > 0) CREATED else DELETED)
    }
  }
}

class DecideMutationOperationBootstrap extends KeyedStateBootstrapFunction[String, Tuple2[String, java.lang.Long]] with LastSeenRevState {
  override def processElement(rev: Tuple2[String, java.lang.Long],
                              context: KeyedStateBootstrapFunction[String, Tuple2[String, java.lang.Long]]#Context): Unit = {
    entityState.updateRevCreate(rev.f1)
  }
}

trait LastSeenRevState extends RichFunction {
  var entityState: EntityState = _

  override def open(parameters: Configuration): Unit = {
    open(new EntityState(getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())))
  }

  def open(entityState: EntityState): Unit = {
    this.entityState = entityState
  }
}




