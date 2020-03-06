package org.wikidata.query.rdf.updater

import scala.collection.mutable.ListBuffer

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec}

trait FlinkTestCluster extends FlatSpec with BeforeAndAfterEach with BeforeAndAfter {
  private val TASK_MANAGER_NO = 1;
  private val TASK_MANAGER_SLOT_NO = 2;
  val PARALLELISM: Int = TASK_MANAGER_NO * TASK_MANAGER_SLOT_NO;

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
}

class CollectSink[E](func: E => Unit) extends SinkFunction[E] {
  override def invoke(value: E, context: SinkFunction.Context[_]): Unit = {
    synchronized {
      func.apply(value)
    }
  }
}

object CollectSink {
  // must be static
  val values: ListBuffer[MutationOperation] = ListBuffer()
  val lateEvents: ListBuffer[InputEvent] = ListBuffer()
  val spuriousRevEvents: ListBuffer[IgnoredMutation] = ListBuffer()
}
