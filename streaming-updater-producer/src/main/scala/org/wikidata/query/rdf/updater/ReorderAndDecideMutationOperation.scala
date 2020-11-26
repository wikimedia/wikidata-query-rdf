package org.wikidata.query.rdf.updater

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

import org.apache.flink.api.common.state.ListState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class ReorderAndDecideMutationOperation(delay: Int) extends KeyedProcessFunction[String, InputEvent, AllMutationOperation] with LastSeenRevState {
  private val LOG = LoggerFactory.getLogger(getClass)

  var bufferedEvents: ListState[InputEvent] = _
  var decideMutationOperation: DecideMutationOperation = new DecideMutationOperation()

  override def processElement(value: InputEvent,
                              ctx: KeyedProcessFunction[String, InputEvent, AllMutationOperation]#Context,
                              out: Collector[AllMutationOperation]): Unit = {
    if (timeToKeep(value) < ctx.timerService().currentWatermark()) {
      ctx.output(EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG, value)
    } else {
      if (shouldBufferEvent(value)) {
        bufferEvent(value, ctx)
      } else {
        fireEvent(out, value)
      }
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, InputEvent, AllMutationOperation]#OnTimerContext,
                       out: Collector[AllMutationOperation]): Unit = {
    val allEvents: Iterable[InputEvent] = Option(bufferedEvents.get()) match {
      case None => List.empty
      case Some(iterable) => iterable.asScala.toList
    }
    if (allEvents.isEmpty) {
      LOG.warn("Received spurious timer on {} at {}", ctx.getCurrentKey, timestamp)
    } else {
      val initialSize = allEvents.size
      // sort based on revision and then event time
      val sorted = allEvents.toList
        .sortBy(e => (e.item, e.revision, e.eventTime))

      val toKeep: List[InputEvent] = fireBufferedEvents(sorted, timestamp, ctx, out)
      if (toKeep.isEmpty) {
        bufferedEvents.clear()
      } else if (initialSize != toKeep.size) {
        // We made progress, update the sate
        bufferedEvents.update(toKeep.asJava)
      } else {
        LOG.warn(s"Made no progress for ${ctx.getCurrentKey} at time $timestamp")
      }
    }
  }

  /**
   * Fire all events whose TTL in the buffer is reached but also
   * the events found *before* those according to the order of the sortedEvent List.
   * @return remaining events that can and need to be kept in the state
   */
  private def fireBufferedEvents(sortedEvents: List[InputEvent],
                                 timestamp: Long,
                                 ctx: KeyedProcessFunction[String, InputEvent, AllMutationOperation]#OnTimerContext,
                                 out: Collector[AllMutationOperation]
                                ): List[InputEvent] = {
    var lastElementToFire: Int = -1

    // find the last element in the buffer that must be fired (timeout)
    for ((e, i) <- sortedEvents.zipWithIndex) {
      if (timeToKeep(e) <= timestamp) {
        lastElementToFire = i
      }
    }

    // fire all elements collected until now that are ordered before the lastElementToFire
    sortedEvents.slice(0, lastElementToFire + 1).foreach(e => {
      fireEvent(out, e)
      deleteUnnecessaryTimer(timestamp, ctx, e)
    })
    val toKeep = sortedEvents.slice(lastElementToFire + 1, sortedEvents.size)
    toKeep
  }

  /**
   * Drop a timer if it's scheduled in the future.
   */
  private def deleteUnnecessaryTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[String, InputEvent, AllMutationOperation]#OnTimerContext,
                                     e: InputEvent
                                    ): Unit = {
    val ttl = timeToKeep(e)
    // drop future timers since we are firing these events
    if (ttl > timestamp) {
      ctx.timerService().deleteEventTimeTimer(ttl)
    }
  }

  /**
   * Timestamp (in event time) at which this event must leave the state.
   */
  private def timeToKeep(e: InputEvent): Long = {
    e.eventTime.toEpochMilli + delay
  }

  private def fireEvent(out: Collector[AllMutationOperation], e: InputEvent): Unit = {
    out.collect(decideMutationOperation.map(e))
  }

  def shouldBufferEvent(value: InputEvent): Boolean = {
    val state = entityState.getCurrentState
    value match {
      case RevCreate(_, _, toRevision, Some(parentRevision), _, _) =>
        state match {
          case State(Some(lastRevision), EntityStatus.CREATED) if lastRevision >= toRevision => false
          case State(Some(lastRevision), EntityStatus.CREATED) if lastRevision == parentRevision && Option(bufferedEvents.get()).isEmpty => false
          case _ => true
        }
      case _ => true
    }
  }

  def bufferEvent(value: InputEvent, ctx: KeyedProcessFunction[String, InputEvent, AllMutationOperation]#Context): Unit = {
    bufferedEvents.add(value)
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + delay)
  }

  override def open(parameters: Configuration): Unit = {
    bufferedEvents = getRuntimeContext.getListState(UpdaterStateConfiguration.newPartialReorderingStateDesc())
    // FIXME: this is ugly
    open(new EntityState(getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())))
    decideMutationOperation.open(entityState)
  }
}

object ReorderAndDecideMutationOperation {
  def attach(stream: DataStream[InputEvent],
             delay: Int,
             uuid: String = DecideMutationOperation.UID): DataStream[AllMutationOperation] = {
    stream
      .keyBy(_.item)
      .process(new ReorderAndDecideMutationOperation(delay))
      // make sure to use the same UUID used by the boostrap job
      .uid(uuid)
      .name("ReorderAndDecideMutationOperation")
  }
}