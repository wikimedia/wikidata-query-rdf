package org.wikidata.query.rdf.updater

import java.io.File
import java.lang
import java.nio.file.Files

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.common.state.{ListState, ValueState}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.{OneInputOperatorTransformation, OperatorTransformation, Savepoint}
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.config.{BaseConfig, StateExtractionConfig}

class StateExtractionJobIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers with BeforeAndAfter {
  private var savePointDir: File = _
  private var checkPointDir: File = _
  private var checkPointDirInStream: File = _
  private var stateInspectionOutput: File = _

  before {
    savePointDir = Files.createTempDirectory("savePoint").toFile
    checkPointDir = Files.createTempDirectory("checkPoint").toFile
    checkPointDirInStream = Files.createTempDirectory("checkPointStream").toFile
    stateInspectionOutput = Files.createTempDirectory("stateInspectionOutput").toFile
  }

  "StateExtractionJob" should "fail if buffered events are found" in {
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(PARALLELISM)
    val newSavepoint = Savepoint.create(UpdaterStateConfiguration.newStateBackend("file:///unused"),
      BaseConfig.MAX_PARALLELISM)
    val bufferedEventOp: OneInputOperatorTransformation[InputEvent] = OperatorTransformation
      .bootstrapWith(env.getJavaEnv.fromCollection(
        Seq[InputEvent](RevCreate("Q1", instant(0), 124, Some(123), instant(1), newEventInfo(instant(0), DOMAIN, STREAM, "REQ_ID"))).asJava,
        InputEventSerializer.typeInfo()
      ))

    val bufferedEventOpTr = bufferedEventOp.keyBy(new KeySelector[InputEvent, String] {
      override def getKey(value: InputEvent): String = value.item
    }).transform(new KeyedStateBootstrapFunction[String, InputEvent] {
      var state: ListState[InputEvent] = _
      var revMap: ValueState[lang.Long] = _
      override def open(parameters: Configuration): Unit = {
        revMap = getRuntimeContext.getState(UpdaterStateConfiguration.newLastRevisionStateDesc())
        state = getRuntimeContext.getListState(UpdaterStateConfiguration.newPartialReorderingStateDesc(true))
      }
      override def processElement(in: InputEvent, context: KeyedStateBootstrapFunction[String, InputEvent]#Context): Unit = {
        state.add(in)
        //state.clear()
        revMap.update(in.revision - 1)
      }
    })
    newSavepoint.withOperator(ReorderAndDecideMutationOperation.UID, bufferedEventOpTr)
      .write(savePointDir.getAbsolutePath)

    env.execute("savepoint")

    val revMapOutput = new File(stateInspectionOutput, "rev_map")
    val stateInspectionConfig = StateExtractionConfig(Seq[String](
      "--checkpoint_dir", "file:///unused",
      "--input_savepoint", savePointDir.getAbsolutePath,
      "--rev_map_output", revMapOutput.toString,
      "--verify", "true", // do not verify as we don't have the buffered events here
      "--job_name", "state inspection",
      "--use_versioned_serializers", "true").toArray)
    StateExtractionJob.configure(stateInspectionConfig)

    try {
      env.execute("state inspection")
      fail("State inspection should fail the verification phase")
    } catch {
      case e: Throwable =>
        ExceptionUtils.getStackTrace(e) should include("Savepoint has 1 buffered event(s) (entity: Q1, revision:123).")
    }
  }
}
