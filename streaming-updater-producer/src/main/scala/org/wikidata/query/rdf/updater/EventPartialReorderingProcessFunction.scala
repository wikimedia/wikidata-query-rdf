package org.wikidata.query.rdf.updater

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}

import org.apache.flink.api.common.state.ListState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class EventPartialReorderingProcessFunction extends KeyedProcessFunction[String, InputEvent, InputEvent] {
  private val LOG = LoggerFactory.getLogger(getClass)

  var bufferedDeletesAndUndeletes: ListState[InputEvent] = _

  override def processElement(value: InputEvent, ctx: KeyedProcessFunction[String, InputEvent, InputEvent]#Context, out: Collector[InputEvent]): Unit = {
    if (value.eventTime.toEpochMilli < ctx.timerService().currentWatermark()) {
      ctx.output(EventReorderingWindowFunction.LATE_EVENTS_SIDE_OUTPUT_TAG, value)
    } else {
      value match {
        case _: RevCreate => fireEvent(value, ctx, out)
        case _ => bufferEvent(value, ctx, out)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, InputEvent, InputEvent]#OnTimerContext, out: Collector[InputEvent]): Unit = {
    val toFire = extractPreviousEvents(timestamp);
    if (toFire.nonEmpty) {
      sortAndSend(toFire, out)
    } else {
      LOG.warn("Received spurious timer on {} at {}", ctx.getCurrentKey, timestamp)
      // spurious timer tigger? we should have deleted this timer
    }
  }

  private def fireEvent(newEvent: InputEvent, ctx: KeyedProcessFunction[String, InputEvent, InputEvent]#Context, out: Collector[InputEvent]): Unit = {
    val toFire: List[InputEvent] = extractPreviousEvents(ctx.timestamp())
    toFire.foreach(e => ctx.timerService().deleteEventTimeTimer(e.eventTime.toEpochMilli))
    sortAndSend(toFire :+ newEvent, out)
  }

  private def sortAndSend(evts: List[InputEvent], out: Collector[InputEvent]): Unit = {
    evts
      .sortBy(e => (e.item, e.revision, e.eventTime))
      .foreach(out.collect)
  }

  private def extractPreviousEvents(timestamp: Long): List[InputEvent] = {
    val allEvents: Iterable[InputEvent] = Option(bufferedDeletesAndUndeletes.get()) match {
      case None => List.empty
      case Some(iterable) => iterable.asScala
    }
    val toFire: List[InputEvent] = allEvents.filter(i => i.eventTime.toEpochMilli <= timestamp).toList
    val toKeep: List[InputEvent] = allEvents.filter(i => i.eventTime.toEpochMilli > timestamp).toList
    if (toKeep.isEmpty) {
      bufferedDeletesAndUndeletes.clear()
    } else if (toKeep.size != allEvents.size) {
      bufferedDeletesAndUndeletes.update(toKeep.asJava)
    }

    toFire
  }

  def bufferEvent(value: InputEvent, ctx: KeyedProcessFunction[String, InputEvent, InputEvent]#Context, out: Collector[InputEvent]): Unit = {
    bufferedDeletesAndUndeletes.add(value)
    ctx.timerService().registerEventTimeTimer(ctx.timestamp())
  }

  override def open(parameters: Configuration): Unit = {
    bufferedDeletesAndUndeletes = getRuntimeContext.getListState(UpdaterStateConfiguration.newPartialReorderingStateDesc())
  }
}

object EventPartialReorderingProcessFunction {
  def attach(stream: DataStream[InputEvent],
             parallelism: Option[Int] = None,
             uuid: String = "EventPartialReordering"): DataStream[InputEvent] = {
    val reorderedStream = stream
      .keyBy(_.item)
      .process(new EventPartialReorderingProcessFunction())
      .uid(uuid)
      .name(uuid)
    parallelism.foreach(reorderedStream.setParallelism)
    reorderedStream
  }
}
