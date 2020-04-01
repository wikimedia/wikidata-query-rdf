package org.wikidata.query.rdf.updater

import java.lang

import org.apache.flink.api.common.functions.{RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

sealed class DecideMutationOperation extends RichMapFunction[InputEvent, AllMutationOperation] with LastSeenRevState {
  override def map(ev: InputEvent): AllMutationOperation = {
    ev match {
      case rev: Rev => {
        val lastRev: lang.Long = state.value()
        if (lastRev == null) {
          state.update(rev.revision)
          FullImport(rev.item, rev.eventTime, rev.revision, rev.ingestionTime)
        } else if (lastRev < rev.revision) {
          state.update(rev.revision)
          Diff(rev.item, rev.eventTime, rev.revision, lastRev, rev.ingestionTime)
        } else {
          // Event related to an old revision
          // It's too late to handle it
          IgnoredMutation(rev.item, rev.eventTime, rev.revision, rev, rev.ingestionTime)
        }
      }
    }
  }
  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
  }
}

trait LastSeenRevState extends RichFunction {
  // use java.lang.Long for nullability
  //  (flink's way for detecting that the state is not set for this entity)
  // there are perhaps better ways to do this
  var state: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
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
  def attach(stream: DataStream[InputEvent]): DataStream[MutationOperation] = {
    stream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .uid(UID)
      .name(UID)
      .process(new RouteIgnoredMutationToSideOutput())
  }
}

class DecideMutationOperationBootstrap extends KeyedStateBootstrapFunction[String, Tuple2[String, java.lang.Long]] with LastSeenRevState {
  override def processElement(rev: Tuple2[String, java.lang.Long],
                              context: KeyedStateBootstrapFunction[String, Tuple2[String, java.lang.Long]]#Context): Unit = {
    state.update(rev.f1)
  }
}

