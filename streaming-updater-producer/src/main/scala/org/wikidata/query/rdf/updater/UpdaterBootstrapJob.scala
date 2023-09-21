package org.wikidata.query.rdf.updater

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.state.api._
import org.apache.flink.state.api.runtime.OperatorIDGenerator
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.updater.config.{BaseConfig, BootstrapConfig}

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
    val newSavepoint = Savepoint.create(UpdaterStateConfiguration.newStateBackend(),
      BaseConfig.MAX_PARALLELISM)

    withRevMap(newSavepoint, settings.revisionsFile)
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

  private def attachOperator[E](newSavepoint: NewSavepoint, op: BootstrapTransformation[E], uid: String) = {
    logger.info(s"Attaching new operator state for [$uid]: " + OperatorIDGenerator.fromUid(uid))
    newSavepoint.withOperator(uid, op)
  }
}
