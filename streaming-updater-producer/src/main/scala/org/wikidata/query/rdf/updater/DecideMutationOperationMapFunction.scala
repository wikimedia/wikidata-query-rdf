package org.wikidata.query.rdf.updater

import scala.math.abs

import org.apache.flink.api.common.functions.{RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.updater.EntityStatus.{CREATED, DELETED, UNDEFINED}

sealed class DecideMutationOperation extends RichMapFunction[InputEvent, AllMutationOperation] with LastSeenRevState {
  override def map(event: InputEvent): AllMutationOperation = {
    event match {
      case rev: RevCreate => mutationFromRevisionCreate(rev)
      case del: PageDelete => mutationFromPageDelete(del)
      case undel: PageUndelete => mutationFromPageUndelete(undel)
      case unknown: InputEvent => emitIgnoredMutation(unknown, NotImplementedYet, entityState.getCurrentState)
    }
  }

  private def mutationFromRevisionCreate(event: RevCreate): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitFullImport(event)
      case State(Some(rev), CREATED) if rev < event.revision => emitDiff(event, rev)
      case state: State =>  emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def mutationFromPageDelete(event: PageDelete): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(Some(rev), CREATED) if rev <= event.revision => emitPageDelete(event)
      case state: State => emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def mutationFromPageUndelete(event: PageUndelete): AllMutationOperation = {
    entityState.getCurrentState match {
      case State(None, _) => emitFullImport(event)
      case State(Some(rev), DELETED) if rev == event.revision => emitFullImport(event)
      case state @ State(Some(rev), DELETED) if rev != event.revision => emitIgnoredMutation(event, UnmatchedUndelete, state)
      case state @ State(Some(rev), CREATED) if rev == event.revision => emitIgnoredMutation(event, UnmatchedUndelete, state)
      case state: State => emitIgnoredMutation(event, NewerRevisionSeen, state)
    }
  }

  private def emitIgnoredMutation(event: InputEvent, inconsistencyType: Inconsistency, state: State): IgnoredMutation = {
    IgnoredMutation(event.item, event.eventTime, event.revision, event, event.ingestionTime, inconsistencyType, state)
  }

  private def emitFullImport(event: InputEvent): FullImport = {
    entityState.updateRevCreate(event.revision)
    FullImport(event.item, event.eventTime, event.revision, event.ingestionTime, event.originalEventMetadata)
  }

  private def emitDiff(event: RevCreate, prevRevision: Long): Diff = {
    entityState.updateRevCreate(event.revision)
    Diff(event.item, event.eventTime, event.revision, prevRevision, event.ingestionTime, event.originalEventMetadata)
  }

  private def emitPageDelete(event: PageDelete): DeleteItem = {
    entityState.updatePageDelete(event.revision)
    DeleteItem(event.item, event.eventTime, event.revision, event.ingestionTime, event.originalEventMetadata)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
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

sealed class RouteIgnoredMutationToSideOutput(ignoredEventTag: OutputTag[IgnoredMutation] = DecideMutationOperation.SPURIOUS_REV_EVENTS)
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

object DecideMutationOperation {
  val UID: String = "DecideMutationOperation"
  val SPURIOUS_REV_EVENTS = new OutputTag[IgnoredMutation]("spurious-rev-events")
  def attach(stream: DataStream[InputEvent]): DataStream[AllMutationOperation] = {
    stream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .uid(UID)
      .name(UID)
  }
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

