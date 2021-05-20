package org.wikidata.query.rdf.updater

import java.lang

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.flink.api.common.state.{ListState, ValueState}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet => ScalaDataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api._
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.util.Collector
import org.wikidata.query.rdf.updater.config.{InputKafkaTopics, StateExtractionConfig}

object StateExtractionJob {
  /**
   * from FlinkKafkaConsumerBase#OFFSETS_STATE_NAME which unfortunately is private
   */
  val offsetsStateName: String = "topic-partition-offset-states"
  val offsetsTypeInfo: TypeInformation[tuple.Tuple2[KafkaTopicPartition, lang.Long]] =
    TypeInformation.of(new TypeHint[tuple.Tuple2[KafkaTopicPartition, lang.Long]] {})

  def main(args: Array[String]): Unit = {
    val config = StateExtractionConfig(args)
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    configure(config)
    env.execute(config.jobName)
  }

  def configure(config: StateExtractionConfig)(implicit env: ExecutionEnvironment): Unit = {
    val stateBackend: StateBackend = UpdaterStateConfiguration.newStateBackend(config.checkpointDir)
    val point: ExistingSavepoint = Savepoint.load(env.getJavaEnv, config.inputSavepoint, stateBackend)
    config.outputRevisionPath.foreach(saveRevisionsAsCsv(point, _, config.verify))
    config.outputKafkaOffsets match {
      case Some(outputPath) =>
        config.inputKafkaTopics match {
          case Some(topics) => saveKafkaOffsets(topics, point, outputPath)
          case None => throw new IllegalArgumentException("Requested to output kafka offsets but input topics not given")
        }
      case None =>
    }
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

  private def loadAndVerifyRevMapDataSet(savepoint: ExistingSavepoint)(implicit env: ExecutionEnvironment): ScalaDataSet[RevMap] = {
    new ScalaDataSet(savepoint.readKeyedState(ReorderAndDecideMutationOperation.UID, new FullDecideMutationSateReader()))
      .map(s => {
        if (s.bufferedEvents.nonEmpty) {
          throw new IllegalStateException(s"Savepoint has ${s.bufferedEvents.size} buffered event(s) " +
            s"(entity: ${s.revMap.key}, revision:${s.revMap.revision}). Savepoint should be created with --drain.")
        }
        s.revMap
      })
  }

  private def saveKafkaOffsets(inputTopics: InputKafkaTopics, savepoint: ExistingSavepoint, outputPath: String)
                              (implicit env: ExecutionEnvironment): Unit = {
    val opIds = inputTopics.topicPrefixes.flatMap { prefix =>
      List(
        inputTopics.revisionCreateTopicName,
        inputTopics.pageUndeleteTopicName,
        inputTopics.pageDeleteTopicName,
        inputTopics.suppressedDeleteTopicName) map { topic => prefix + topic }
    } map IncomingStreams.operatorUUID

    opIds map { uuid =>
      (uuid, kafkaOffsetsDataset(savepoint, uuid) map { t =>
        (t.f0.getTopic, String.valueOf(t.f0.getPartition), String.valueOf(t.f1))
      })} foreach { d =>
        d._2.setParallelism(1).writeAsCsv(outputPath + "/" + d._1 + ".csv").setParallelism(1)
    }
  }

  def kafkaOffsetsDataset(savepoint: ExistingSavepoint, uuid: String)
                         (implicit env: ExecutionEnvironment): ScalaDataSet[Tuple2[KafkaTopicPartition, lang.Long]] = {
    // FIXME: this does not work, this dataset remains empty when applied to current savepoints
    new ScalaDataSet(savepoint.readUnionState(uuid, offsetsStateName, offsetsTypeInfo, createStateSerializer(env.getConfig))).setParallelism(1)
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

  class FullDecideMutationSateReader extends KeyedStateReaderFunction[String, FullDecideMutationState] {
    var revState: ValueState[java.lang.Long] = _
    var bufferedEvents: ListState[InputEvent] = _

    override def open(parameters: Configuration): Unit = {
      revState = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
      bufferedEvents = getRuntimeContext.getListState(UpdaterStateConfiguration.newPartialReorderingStateDesc())
    }

    override def readKey(k: String, context: KeyedStateReaderFunction.Context, collector: Collector[FullDecideMutationState]): Unit = {
      val revMap = RevMap(k, revState.value())
      collector.collect(FullDecideMutationState(revMap, bufferedEvents.get().asScala.toList))
    }
  }


  private def createStateSerializer(executionConfig: ExecutionConfig): TupleSerializer[Tuple2[KafkaTopicPartition, lang.Long]] = {
    val fieldSerializers = Array[TypeSerializer[_]](new KryoSerializer[KafkaTopicPartition](classOf[KafkaTopicPartition], executionConfig),
      LongSerializer.INSTANCE)
    val tupleClass = classOf[Tuple2[KafkaTopicPartition, lang.Long]]
    new TupleSerializer[Tuple2[KafkaTopicPartition, lang.Long]](tupleClass, fieldSerializers)
  }


}
