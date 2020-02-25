package org.wikidata.query.rdf

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, StreamExecutionEnvironment}
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import scala.collection.mutable.ListBuffer

class LetterCountJobIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  private val TASK_MANAGER_NO = 1;
  private val TASK_MANAGER_SLOT_NO = 1;

  private val PARALLELISM = TASK_MANAGER_NO * TASK_MANAGER_SLOT_NO;

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(TASK_MANAGER_NO)
    .setNumberTaskManagers(TASK_MANAGER_SLOT_NO)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "LetterCountJob pipeline" should "provide words with letter counts" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(PARALLELISM)

    // values are collected in a static variable
    CollectSink.values.clear()

    val input = Seq("the", "quick", "fox")
    // create a stream of custom elements and apply transformations
    val source = env.fromCollection(input)

    LetterCountJob.process(source, new CollectSink())

    // execute
    env.execute()

    // verify your results
    CollectSink.values should contain allOf (("the", 3), ("quick", 5), ("fox", 3))
  }
}
// create a testing sink
class CollectSink extends SinkFunction[(String, Int)] {

  override def invoke(value: (String, Int)): Unit = {
    synchronized {
      CollectSink.values += value
    }
  }
}

object CollectSink {
  // must be static
  val values: ListBuffer[(String, Int)] = ListBuffer()
}
