package org.wikidata.query.rdf.updater

import java.lang

import scala.language.postfixOps

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.{Tuple2, Tuple3}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.state.api._
import org.apache.flink.state.api.functions.StateBootstrapFunction
import org.apache.flink.state.api.runtime.OperatorIDGenerator
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.updater.config.{BaseConfig, BootstrapConfig, InputKafkaTopics}

object UpdaterBootstrapJob {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    val settings = BootstrapConfig(args)
    // Unclear but changing the parallelism of a keyed state might not be a "free" operation so I think it's better to
    // match the parallelism of the stream operation when building the initial savepoint
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(settings.parallelism)

    newSavepoint(settings).write(settings.savepointDir)
    env.execute(settings.jobName)
  }

  def newSavepoint(settings: BootstrapConfig)(implicit env: ExecutionEnvironment): NewSavepoint = {
    val kafkaOffsets: Map[String, JavaDataSet[Tuple3[String, Integer, lang.Long]]] = settings.kafkaOffsetsFolder match {
      case Some(csvFolder) => collectKafkaOffsets(csvFolder, settings.inputKafkaTopics.get)
      case None => Map.empty[String, JavaDataSet[Tuple3[String, Integer, lang.Long]]]
    }

    val newSavepoint = Savepoint.create(UpdaterStateConfiguration.newStateBackend(),
      BaseConfig.MAX_PARALLELISM)

    withRevMap(newSavepoint, settings.revisionsFile)
    kafkaOffsets foreach { case (topic, tpo) => withKafkaOffsets(newSavepoint, topic, tpo) }
    newSavepoint
  }

  private def dataSet(dataSet: JavaDataSet[Tuple2[String, java.lang.Long]])
                     (implicit env: ExecutionEnvironment): BootstrapTransformation[Tuple2[String, java.lang.Long]] = {
    val op: OneInputOperatorTransformation[Tuple2[String, java.lang.Long]] = OperatorTransformation.bootstrapWith(dataSet)
    op.keyBy(new KeySelector[Tuple2[String, java.lang.Long], String]() {
        override def getKey(value: Tuple2[String, java.lang.Long]): String = value.f0
      })
      .transform(new DecideMutationOperationBootstrap())
  }

  private def fromCsv(path: String)(implicit env: ExecutionEnvironment): JavaDataSet[Tuple2[String, java.lang.Long]] = {
    env.getJavaEnv.readCsvFile(path)
      .types(classOf[String], classOf[java.lang.Long])
  }

  def withRevMap(newSavepoint: NewSavepoint, revFile: String)(implicit env: ExecutionEnvironment): NewSavepoint = {
    attachOperator(newSavepoint, dataSet(fromCsv(revFile)), ReorderAndDecideMutationOperation.UID)
  }

  def collectKafkaOffsets(kafkaOffsetsFolder: String, inputTopics: InputKafkaTopics)
                         (implicit env: ExecutionEnvironment): Map[String, JavaDataSet[Tuple3[String, lang.Integer, lang.Long]]] = {
    val topics = Seq(
      inputTopics.revisionCreateTopicName,
      inputTopics.pageUndeleteTopicName,
      inputTopics.pageDeleteTopicName,
      inputTopics.suppressedDeleteTopicName
    ) flatMap(t => {
      inputTopics.topicPrefixes.map(_ + t)
    })
    topics map (t => {
      (t, env.getJavaEnv.readCsvFile(kafkaOffsetsFolder + "/" + t + ".csv")
        .types(classOf[String], classOf[lang.Integer], classOf[lang.Long]): JavaDataSet[Tuple3[String, lang.Integer, lang.Long]])
    }) toMap
  }

  def withKafkaOffsets(newSavepoint: NewSavepoint, topic: String, topicPartitionOffsets: JavaDataSet[Tuple3[String, lang.Integer, lang.Long]])
                      (implicit env: ExecutionEnvironment): NewSavepoint = {
    val dataSet: JavaDataSet[Tuple2[KafkaTopicPartition, lang.Long]] = topicPartitionOffsets.map(
      new MapFunction[Tuple3[String, Integer, lang.Long], Tuple2[KafkaTopicPartition, lang.Long]]() {
        override def map(t: Tuple3[String, Integer, lang.Long]): Tuple2[KafkaTopicPartition, lang.Long] = {
          new Tuple2[KafkaTopicPartition, lang.Long](new KafkaTopicPartition(t.f0, t.f1), t.f2)
        }
      })
    val op = OperatorTransformation.bootstrapWith(dataSet)
      .transform(new StateBootstrapFunction[Tuple2[KafkaTopicPartition, lang.Long]] {
        var unionState: ListState[Tuple2[KafkaTopicPartition, lang.Long]] = _
        override def processElement(in: Tuple2[KafkaTopicPartition, lang.Long], context: StateBootstrapFunction.Context): Unit = {
          unionState.add(in)
        }

        override def snapshotState(context: FunctionSnapshotContext): Unit = {}

        override def initializeState(context: FunctionInitializationContext): Unit = {
          unionState = context.getOperatorStateStore
            .getUnionListState(new ListStateDescriptor(StateExtractionJob.offsetsStateName, StateExtractionJob.offsetsTypeInfo))
        }
      })
    attachOperator(newSavepoint, op, IncomingStreams.operatorUUID(topic))
  }

  private def attachOperator[E](newSavepoint: NewSavepoint, op: BootstrapTransformation[E], uid: String) = {
    logger.info(s"Attaching new operator state for [$uid]: " + OperatorIDGenerator.fromUid(uid))
    newSavepoint.withOperator(IncomingStreams.operatorUUID(uid), op)
  }
}
