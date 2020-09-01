package org.wikidata.query.rdf.updater

import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api.{BootstrapTransformation, NewSavepoint, OneInputOperatorTransformation, OperatorTransformation, Savepoint}

object UpdaterBootstrapJob {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    // TODO: write better (as in more reusable) code for option handling
    val csvFile = Option(params.get("revisions_file")) match {
      case Some(csvFile) => csvFile;
      case None => throw new IllegalArgumentException("""missing param revisions_file""")
    }
    val savepointDir = Option(params.get("savepoint_dir")) match {
      case Some(savePointDir) => savePointDir;
      case None => throw new IllegalArgumentException("""missing param savepoint_dir""")
    }
    val checkpointDir = Option(params.get("checkpoint_dir")) match {
      case Some(checkpointDir) => checkpointDir;
      case None => throw new IllegalArgumentException("""missing param checkpoint_dir""")
    }
    val parallelism = Option(params.getInt("parallelism")) match {
      case Some(p) => p;
      case None => throw new IllegalArgumentException("""missing param parallelism""")
    }
    // Unclear but changing the parallelism of a keyed state might not be a "free" operation so I think it's better to
    // match the parallelism of the stream operation when building the initial savepoint
    implicit val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    newSavePoint(csvFile, UpdaterStateConfiguration.newStateBackend(checkpointDir), parallelism)
      .write(savepointDir)
    env.execute("WDQS Updater Bootstrap Job")
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

  def newSavePoint(revFile: String, stateBackend: StateBackend, parallelism: Int)(implicit env: ExecutionEnvironment): NewSavepoint = {
    Savepoint.create(stateBackend, parallelism)
      .withOperator(DecideMutationOperation.UID, dataSet(fromCsv(revFile)))
  }
}
