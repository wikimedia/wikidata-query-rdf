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

  "A savepoint with kafka offsets and revisions map" should "be convertible to a set of csv files" in {
    val csvFile = this.getClass.getResource("/bootstrap_revisions.csv").toString
    val kafkaOffsets = this.getClass.getResource("/bootstrap_kafkaoffsets").toString

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
      "--kafka_offsets_folder", kafkaOffsets,
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
    val kafkaOffsetsOutput = new File(stateInspectionOutput, "kafka_offsets")

    val stateInspectionConfig = StateExtractionConfig(Seq[String](
      "--checkpoint_dir", "file:///unused",
      "--input_savepoint", savePointDir.getAbsolutePath,
      "--rev_map_output", revFileOutput.toString,
      "--verify", "false", // do not verify as we don't have the buffered events here
      "--kafka_offsets_output", kafkaOffsetsOutput.toString,
      "--job_name", "state inspection").toArray ++ topics)
    StateExtractionJob.configure(stateInspectionConfig)
    env.execute("state inspection")


    val actualRevCreateOffsets: DataSet[(String, String, String)] = env.readCsvFile(kafkaOffsetsOutput + "/" + "datacenter.rev_create.topic.csv")
    val actualDataSet: DataSet[(String, String, String)] = actualRevCreateOffsets
      .union(env.readCsvFile(kafkaOffsetsOutput + "/" + "datacenter.page_delete.topic.csv"): DataSet[(String, String, String)])
      .union(env.readCsvFile(kafkaOffsetsOutput + "/" + "datacenter.page_undelete.topic.csv"): DataSet[(String, String, String)])
      .union(env.readCsvFile(kafkaOffsetsOutput + "/" + "datacenter.page_suppress.topic.csv"): DataSet[(String, String, String)])

    val expectedRevCreateOffsets: DataSet[(String, String, String)] = env.readCsvFile(kafkaOffsets + "/" + "datacenter.rev_create.topic.csv")
    val expectedDataSet: DataSet[(String, String, String)] = expectedRevCreateOffsets
      .union(env.readCsvFile(kafkaOffsets + "/" + "datacenter.page_delete.topic.csv"): DataSet[(String, String, String)])
      .union(env.readCsvFile(kafkaOffsets + "/" + "datacenter.page_undelete.topic.csv"): DataSet[(String, String, String)])
      .union(env.readCsvFile(kafkaOffsets + "/" + "datacenter.page_suppress.topic.csv"): DataSet[(String, String, String)])

    val actualOffsets = actualDataSet.collect()
    val expectedOffsets = expectedDataSet.collect()
    actualOffsets.size shouldEqual 3
    actualOffsets should contain theSameElementsAs expectedOffsets

    val expectedRevMap: DataSet[(String, String)] = env.readCsvFile(csvFile)
    val expectedRevisions = expectedRevMap.collect()

    val actualRevMap: DataSet[(String, String)] = env.readCsvFile(revFileOutput.toString)
    val actualRevisions = actualRevMap.collect()

    actualOffsets.size shouldEqual 3
    actualRevisions should contain theSameElementsAs expectedRevisions
  }

  after {
    FileUtils.forceDelete(savePointDir)
    FileUtils.forceDelete(checkPointDir)
    FileUtils.forceDelete(checkPointDirInStream)
    FileUtils.forceDelete(stateInspectionOutput)
  }
}
