package org.wikidata.query.rdf.updater

import scala.math.abs

import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.updater.EntityStatus.{CREATED, DELETED, UNDEFINED}

sealed class MutationResolver extends Serializable {
  def map(event: InputEvent, entityState: EntityState): AllMutationOperation = {
    event match {
      case rev: RevCreate => mutationFromRevisionCreate(rev, entityState)
      case del: PageDelete => mutationFromPageDelete(del, entityState)
      case undel: PageUndelete => mutationFromPageUndelete(undel, entityState)
      case unknown: InputEvent => emitIgnoredMutation(unknown, NotImplementedYet, entityState.getCurrentState)
    }
  }

  private def mutationFromRevisionCreate(event: RevCreate, entityState: EntityState): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitFullImport(event, entityState)
      case State(Some(rev), CREATED) if rev < event.revision => emitDiff(event, rev, entityState)
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

  private def emitIgnoredMutation(event: InputEvent, inconsistencyType: Inconsistency, state: State): IgnoredMutation = {
    IgnoredMutation(event.item, event.eventTime, event.revision, event, event.ingestionTime, inconsistencyType, state)
  }

  private def emitFullImport(event: InputEvent, entityState: EntityState): FullImport = {
    entityState.updateRevCreate(event.revision)
    FullImport(event.item, event.eventTime, event.revision, event.ingestionTime, event.originalEventMetadata)
  }

  private def emitDiff(event: RevCreate, prevRevision: Long, entityState: EntityState): Diff = {
    entityState.updateRevCreate(event.revision)
    Diff(event.item, event.eventTime, event.revision, prevRevision, event.ingestionTime, event.originalEventMetadata)
  }

  private def emitPageDelete(event: PageDelete, entityState: EntityState): DeleteItem = {
    entityState.updatePageDelete(event.revision)
    DeleteItem(event.item, event.eventTime, event.revision, event.ingestionTime, event.originalEventMetadata)
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

sealed class RouteIgnoredMutationToSideOutput(ignoredEventTag: OutputTag[IgnoredMutation] = MutationResolver.SPURIOUS_REV_EVENTS)
  extends ProcessFunction[AllMutationOperation, MutationOperation]
{
  override def processElement(i: AllMutationOperation,
                              context: ProcessFunction[AllMutationOperation, MutationOperation]#Context,
                              collector: Collector[MutationOperation]
                             ): Unit = {
    i match {
      case e: IgnoredMutation => context.output(ignoredEventTag, e)
      case x: MutationOperation => collector.collect(x)
    }
  }
}

object MutationResolver {
  val UID: String = "DecideMutationOperation"
  val SPURIOUS_REV_EVENTS = new OutputTag[IgnoredMutation]("spurious-rev-events")
}

class EntityState(revState: ValueState[java.lang.Long]) {
  def updatePageDelete(revision: Long): Unit = revState.update(-revision)
  def updateRevCreate(revision: Long): Unit = revState.update(revision)

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

