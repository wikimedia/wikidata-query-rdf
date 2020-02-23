package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

sealed class DecideMutationOperation extends RichMapFunction[InputEvent, AllMutationOperation] {
  // FIXME: use java.lang.Long for nullability
  //  (flink's way for detecting that the state is not set for this entity)
  private var state: ValueState[java.lang.Long] = _
  override def map(ev: InputEvent): AllMutationOperation = {
    ev match {
      case rev: Rev => {
        val lastRev = state.value()
        if (lastRev == null) {
          state.update(rev.revision)
          FullImport(rev.item, rev.eventTime, rev.revision)
        } else if (lastRev <= rev.revision) {
          state.update(rev.revision)
          Diff(rev.item, rev.eventTime, rev.revision, lastRev)
        } else {
          // Event related to an old revision
          // It's too late to handle it
          IgnoredMutation(rev.item, rev.eventTime, rev.revision, rev)
        }
      }
    }
  }
  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(UpdaterStateDescriptors.newLastRevisionStateDesc())
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
  val SPURIOUS_REV_EVENTS = new OutputTag[IgnoredMutation]("spurious-rev-events")
  def attach(stream: DataStream[InputEvent]): DataStream[MutationOperation] = {
    stream
      .keyBy(_.item)
      .map(new DecideMutationOperation())
      .uid("DecideMutationOperation")
      .process(new RouteIgnoredMutationToSideOutput())
  }
}

