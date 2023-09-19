package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.state.{ListState, ValueState}
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet => ScalaDataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api._
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.updater.config.StateExtractionConfig

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object StateExtractionJob {
  def main(args: Array[String]): Unit = {
    val config = StateExtractionConfig(args)
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    configure(config)
    env.execute(config.jobName)
  }

  def configure(config: StateExtractionConfig)(implicit env: ExecutionEnvironment): Unit = {
    val stateBackend: StateBackend = UpdaterStateConfiguration.newStateBackend()
    val point: ExistingSavepoint = Savepoint.load(env.getJavaEnv, config.inputSavepoint, stateBackend)
    config.outputRevisionPath.foreach(saveRevisionsAsCsv(point, _, config.verify))
  }

  def saveRevisionsAsCsv(savepoint: ExistingSavepoint, outputPath: String, verify: Boolean)
                        (implicit env: ExecutionEnvironment): Unit = {
    revisionMapDataset(savepoint, verify).writeAsCsv(outputPath)
  }

  def revisionMapDataset(savepoint: ExistingSavepoint, verify: Boolean)
                        (implicit env: ExecutionEnvironment): ScalaDataSet[(String, String)] = {
    val dataset = if (verify) loadAndVerifyRevMapDataSet(savepoint) else loadRevMapDataSet(savepoint)
    dataset
      .map(revMap => (revMap.key, String.valueOf(revMap.revision)))
  }

  private def loadRevMapDataSet(savepoint: ExistingSavepoint)(implicit env: ExecutionEnvironment): ScalaDataSet[RevMap] = {
    new ScalaDataSet(savepoint.readKeyedState(ReorderAndDecideMutationOperation.UID, new RevMapStateReader()))
  }

  private def loadAndVerifyRevMapDataSet(savepoint: ExistingSavepoint)
                                        (implicit env: ExecutionEnvironment): ScalaDataSet[RevMap] = {
    new ScalaDataSet(savepoint.readKeyedState(ReorderAndDecideMutationOperation.UID, new FullDecideMutationSateReader()))
      .map(s => {
        if (s.bufferedEvents.nonEmpty) {
          throw new IllegalStateException(s"Savepoint has ${s.bufferedEvents.size} buffered event(s) " +
            s"(entity: ${s.revMap.key}, revision:${s.revMap.revision}). Savepoint should be created with --drain.")
        }
        s.revMap
      })
  }

  case class RevMap(key: String, revision: java.lang.Long)
  case class FullDecideMutationState(revMap: RevMap, bufferedEvents: List[InputEvent])

  class RevMapStateReader extends KeyedStateReaderFunction[String, RevMap] {
    var revState: ValueState[java.lang.Long] = _

    override def open(parameters: Configuration): Unit = {
      revState = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
    }

    override def readKey(k: String, context: KeyedStateReaderFunction.Context, collector: Collector[RevMap]): Unit = {
      collector.collect(RevMap(k, revState.value()))
    }
  }

  private class FullDecideMutationSateReader() extends KeyedStateReaderFunction[String, FullDecideMutationState] {
    private var revState: ValueState[java.lang.Long] = _
    private var bufferedEvents: ListState[InputEvent] = _

    override def open(parameters: Configuration): Unit = {
      revState = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
      bufferedEvents = getRuntimeContext.getListState(UpdaterStateConfiguration.newPartialReorderingStateDesc())
    }

    override def readKey(k: String, context: KeyedStateReaderFunction.Context, collector: Collector[FullDecideMutationState]): Unit = {
      val revMap = RevMap(k, revState.value())
      collector.collect(FullDecideMutationState(revMap, bufferedEvents.get().asScala.toList))
    }
  }
}
