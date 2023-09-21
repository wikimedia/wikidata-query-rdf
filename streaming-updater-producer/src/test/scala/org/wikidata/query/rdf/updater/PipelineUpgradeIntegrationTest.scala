package org.wikidata.query.rdf.updater

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.updater.config.{BootstrapConfig, StateExtractionConfig}

class PipelineUpgradeIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers with BeforeAndAfter {
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

  "A savepoint with revisions map" should "be convertible to a set of csv files" in {
    val csvFile = this.getClass.getResource("/bootstrap_revisions.csv").toString

    val topics = Seq[String](
      "--rev_create_topic", "rev_create.topic",
      "--page_delete_topic", "page_delete.topic",
      "--page_undelete_topic", "page_undelete.topic",
      "--suppressed_delete_topic", "page_suppress.topic",
      "--topic_prefixes", "datacenter."
    )

    val config = BootstrapConfig(Seq[String](
      "--job_name", "bootstrap",
      "--checkpoint_dir", "file:///unused",
      "--revisions_file", csvFile,
      "--savepoint_dir", savePointDir.getAbsolutePath,
      "--parallelism", String.valueOf(PARALLELISM)
    ).toArray ++ topics)

    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // configure your test environment
    env.setParallelism(PARALLELISM)

    UpdaterBootstrapJob.newSavepoint(config)
      .write(savePointDir.getAbsolutePath)
    env.execute("write savepoint")

    val revFileOutput = new File(stateInspectionOutput, "rev_map_out.csv")

    val stateInspectionConfig = StateExtractionConfig(Seq[String](
      "--checkpoint_dir", "file:///unused",
      "--input_savepoint", savePointDir.getAbsolutePath,
      "--rev_map_output", revFileOutput.toString,
      "--verify", "false", // do not verify as we don't have the buffered events here
      "--job_name", "state inspection").toArray ++ topics)
    StateExtractionJob.configure(stateInspectionConfig)
    env.execute("state inspection")


    val expectedRevMap: DataSet[(String, String)] = env.readCsvFile(csvFile)
    val expectedRevisions = expectedRevMap.collect()

    val actualRevMap: DataSet[(String, String)] = env.readCsvFile(revFileOutput.toString)
    val actualRevisions = actualRevMap.collect()

    actualRevisions should contain theSameElementsAs expectedRevisions
  }

  after {
    FileUtils.forceDelete(savePointDir)
    FileUtils.forceDelete(checkPointDir)
    FileUtils.forceDelete(checkPointDirInStream)
    FileUtils.forceDelete(stateInspectionOutput)
  }
}
