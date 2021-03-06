package org.wikidata.query.rdf.updater

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.RevisionCreateEvent
import org.wikidata.query.rdf.updater.EntityStatus.CREATED
import org.wikidata.query.rdf.updater.config.{BootstrapConfig, HttpClientConfig, UpdaterPipelineGeneralConfig}


class UpdaterBootstrapJobIntegrationTest extends FlatSpec with FlinkTestCluster with TestFixtures with Matchers with BeforeAndAfter {
  private var savePointDir: File = _
  private var checkPointDir: File = _
  private var checkPointDirInStream: File = _


  before {
    savePointDir = Files.createTempDirectory("savePoint").toFile
    checkPointDir = Files.createTempDirectory("checkPoint").toFile
    checkPointDirInStream = Files.createTempDirectory("checkPointStream").toFile
  }

  "a savepoint" should "created by loading a csv file with entity revisions" in {
    implicit val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // configure your test environment
    env.setParallelism(PARALLELISM)

    val csvFile = this.getClass.getResource("/bootstrap_revisions.csv").toString

    val config = BootstrapConfig(Seq[String](
    "--job_name", "bootstrap",
      "--checkpoint_dir", "file:///unused",
      "--revisions_file", csvFile,
      "--savepoint_dir", savePointDir.getAbsolutePath,
      "--parallelism", String.valueOf(PARALLELISM)
    ).toArray)
    UpdaterBootstrapJob.newSavepoint(config)
      .write(savePointDir.getAbsolutePath)
    env.execute("write savepoint")
    val metatadaFile = Paths.get(savePointDir.getAbsolutePath, "_metadata").toFile
    metatadaFile.exists() should equal(true)

    implicit val streamingEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    streamingEnv.setParallelism(PARALLELISM)
    streamingEnv.setStateBackend(UpdaterStateConfiguration.newStateBackend(checkPointDirInStream.toURI.toString))
    val repository: WikibaseEntityRevRepositoryTrait = MockWikibaseEntityRevRepository()
      .withResponse(("Q1", 2L) -> metaStatements("Q1", 2L, Some(3L)).entityDataNS)
      .withResponse(("Q1", 3L) -> metaStatements("Q1", 3L, Some(3L)).entityDataNS)
      .withResponse(("Q2", 4L) -> metaStatements("Q2", 4L, Some(3L)).entityDataNS)
      .withResponse(("Q2", 8L) -> metaStatements("Q2", 8L, Some(3L)).entityDataNS)
      .withResponse(("Q3", 101010L) -> metaStatements("Q3", 101010L, Some(3L)).entityDataNS)
      .withResponse(("Q3", 101013L) -> metaStatements("Q3", 101013L, Some(3L)).entityDataNS)

    val input = Seq(
      newRevCreateEvent("Q1", 2, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID), // dupped event, currently treated as spurious
      newRevCreateEvent("Q1", 3, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID),
      newRevCreateEvent("Q2", 8, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID),
      newRevCreateEvent("Q3", 101013, instant(3), 0, DOMAIN, STREAM, ORIG_REQUEST_ID)
    )

    val source: DataStream[InputEvent] = IncomingStreams.fromStream(streamingEnv.fromCollection(input)
      .assignTimestampsAndWatermarks(watermarkStrategy[RevisionCreateEvent]()),
      URIS,
      IncomingStreams.REV_CREATE_CONV,
      clock)

    val options = UpdaterPipelineGeneralConfig(
      hostname = DOMAIN,
      jobName = "test updater job",
      entityNamespaces = ENTITY_NAMESPACES,
      entityDataPath = "/Special:EntityData",
      reorderingWindowLengthMs = 60000,
      generateDiffTimeout = Int.MaxValue,
      wikibaseRepoThreadPoolSize = 10,
      outputOperatorNameAndUuid = "test-output-name",
      httpClientConfig = HttpClientConfig(None, None, "my user-agent"),
      useVersionedSerializers = true
    )
    UpdaterPipeline.configure(options, List(source),
      OutputStreams(
        new CollectSink[MutationDataChunk](CollectSink.values.append(_)),
        new CollectSink[InputEvent](CollectSink.lateEvents.append(_)),
        new CollectSink[IgnoredMutation](CollectSink.spuriousRevEvents.append(_))
      ),
      _ => repository, clock = clock)
    val graph = streamingEnv.getStreamGraph("test")
    graph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savePointDir.toURI.toString, false))
    streamingEnv.getJavaEnv.execute(graph)
    CollectSink.lateEvents shouldBe empty
    CollectSink.spuriousRevEvents should contain only IgnoredMutation("Q1", instant(3), 2,
      RevCreate("Q1", instant(3), 2, None, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      instantNow, NewerRevisionSeen, State(Some(2), CREATED))
    //only change is the revision, lastmodified are identical

    CollectSink.values map {_.operation} should contain theSameElementsAs Vector(
      Diff("Q1", instant(3), 3, 2, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      Diff("Q2", instant(3), 8, 4, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID)),
      Diff("Q3", instant(3), 101013, 101010, instantNow, newEventInfo(instant(3), DOMAIN, STREAM, ORIG_REQUEST_ID))
    )
  }


  after {
    FileUtils.forceDelete(savePointDir)
    FileUtils.forceDelete(checkPointDir)
    FileUtils.forceDelete(checkPointDirInStream)
  }
}
