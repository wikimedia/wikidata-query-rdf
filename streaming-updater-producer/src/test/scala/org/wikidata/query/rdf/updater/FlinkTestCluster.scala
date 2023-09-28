package org.wikidata.query.rdf.updater

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec}

trait FlinkTestCluster extends FlatSpec with BeforeAndAfterEach with BeforeAndAfter {
  private val TASK_MANAGER_NO = 1
  private val TASK_MANAGER_SLOT_NO = 2
  val PARALLELISM: Int = TASK_MANAGER_NO * TASK_MANAGER_SLOT_NO

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(TASK_MANAGER_NO)
    .setNumberTaskManagers(TASK_MANAGER_SLOT_NO)
    .build)

  override def beforeEach(): Unit = {
    CollectSink.values.clear()
    CollectSink.lateEvents.clear()
    CollectSink.spuriousRevEvents.clear()
    flinkCluster.before()
  }

  override def afterEach(): Unit = {
    flinkCluster.after()
  }

  def identityMapFunction[O](): MapFunction[O, O] = new MapFunction[O, O] {
    override def map(t: O): O = t
  }
}

class CollectSink[E](func: E => Unit) extends SinkFunction[E] {
  override def invoke(value: E, context: SinkFunction.Context): Unit = {
    synchronized {
      func.apply(value)
    }
  }
}

object CollectSink {
  // must be static
  val values: ListBuffer[MutationDataChunk] = ListBuffer()
  val lateEvents: ListBuffer[InputEvent] = ListBuffer()
  val spuriousRevEvents: ListBuffer[InconsistentMutation] = ListBuffer()

  def asOutputStreams: OutputStreams = {
    OutputStreams(
      SinkWrapper(Left(new CollectSink[MutationDataChunk](CollectSink.values.append(_))), "mutations"),
      Some(SinkWrapper(Left(new CollectSink[InputEvent](CollectSink.lateEvents.append(_))), "late-events")),
      Some(SinkWrapper(Left(new CollectSink[InconsistentMutation](CollectSink.spuriousRevEvents.append(_))), "inconsistencies")),
      None
    )
  }
}
